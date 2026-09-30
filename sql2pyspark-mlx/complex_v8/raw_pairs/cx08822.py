from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_agent_id_priority_by_request = Window.partitionBy("agent_id", "priority").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_priority_by_request_2 = Window.partitionBy("agent_id", "priority").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_requester_id_ful = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_requester_id_mov = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-3, Window.currentRow)

recent = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-13 13:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "tags",
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

metrics = (
    recent
    .select(
        "id",
        "requester_id",
        "priority",
        "tags",
        "priority_clean",
        F.first("requester_id", ignorenulls=True).over(w_agent_id_priority_by_request).alias("first_known_requester_id"),
        F.nth_value("requester_id", 2).over(w_agent_id_priority_by_request_2).alias("second_requester_id"),
        F.nth_value("requester_id", 2).over(w_agent_id_by_requester_id_ful).alias("second_requester_id_2"),
        F.max("requester_id").over(w_agent_id_by_requester_id_mov).alias("requester_id_max_4"),
    )
)

priority_stats = (
    metrics
    .groupBy("priority")
    .agg(F.percentile_approx("second_requester_id", 0.5).alias("median_second_requester_id"))
)

outliers = (
    metrics.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.second_requester_id") >= F.col("g.median_second_requester_id"))
    .select(
        "s.*",
        "g.median_second_requester_id",
        F.round(F.col("s.second_requester_id") - F.col("g.median_second_requester_id"), 2).alias("second_requester_id_minus_median"),
    )
)

result = (
    outliers
    .select(
        "id",
        "requester_id_max_4",
        "first_known_requester_id",
        "second_requester_id",
        "requester_id",
    )
    .unpivot(
        ids=["id"],
        values=["requester_id_max_4", "first_known_requester_id", "second_requester_id", "requester_id"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
