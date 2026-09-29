from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_agent_id_priority_by_opened = Window.partitionBy("agent_id", "priority").orderBy("opened_at", "id")
w_csat_priority_by_csat_change = Window.partitionBy("csat", "priority").orderBy(F.col("csat_change_per_csat").desc_nulls_first(), "id")
w_csat_priority_by_csat_change_2 = Window.partitionBy("csat", "priority").orderBy(F.col("csat_change_per_csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
    )
)

with_trends = (
    recent_tickets
    .select(
        "id",
        "requester_id",
        "priority",
        "csat",
        F.round(F.col("requester_id_plus_csat") / F.nullif(F.sum("requester_id_plus_csat").over(w_priority_agent_id), F.lit(0)), 4).alias("requester_id_plus_csat_share_of_priority_agent_i"),
        (F.col("csat") - F.lag("csat").over(w_agent_id_priority_by_opened)).alias("csat_change"),
    )
)

qualified = (
    with_trends
    .crossJoin(with_trends.agg(F.max("requester_id_plus_csat_share_of_priority_agent_i").alias("overall_max_requester_id_plus_csat_share_of_prio")))
    .filter(F.col("requester_id_plus_csat_share_of_priority_agent_i") < F.col("overall_max_requester_id_plus_csat_share_of_prio"))
    .drop("overall_max_requester_id_plus_csat_share_of_prio")
)

scored = (
    qualified
    .filter(F.col("priority") == "P4")
    .select(
        "*",
        F.round(F.col("csat_change") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("csat_change_per_csat"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

priority_stats = (
    scored
    .groupBy("priority")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

outliers = (
    scored.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

with_trends_2 = (
    outliers
    .select(
        "*",
        (F.col("priority_len") - F.lag("priority_len").over(w_csat_priority_by_csat_change)).alias("priority_len_change"),
        F.first("csat", ignorenulls=True).over(w_csat_priority_by_csat_change_2).alias("first_known_csat"),
        F.round(F.cume_dist().over(w_csat_priority_by_csat_change), 3).alias("csat_change_per_csat_cume_dist"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "requester_id_minus_avg",
        "requester_id_plus_csat_share_of_priority_agent_i",
        "csat_change_per_csat",
    )
    .unpivot(
        ids=["id"],
        values=["requester_id_minus_avg", "requester_id_plus_csat_share_of_priority_agent_i", "csat_change_per_csat"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
