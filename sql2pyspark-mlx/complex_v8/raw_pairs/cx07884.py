from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_starts_session_by_prev_close = Window.partitionBy("starts_session").orderBy(F.col("prev_closed_at").desc())

filtered_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

with_prev = filtered_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_agent_id_running))

metrics = (
    sessionized
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "prev_closed_at",
        "starts_session",
        "session_id",
        F.first("csat").over(w_all_by_closed_at_full).alias("first_csat"),
        (F.col("starts_session") - F.lag("starts_session").over(w_all_by_closed_at)).alias("starts_session_change"),
        F.nth_value("csat", 2).over(w_all_by_closed_at_full).alias("second_csat"),
        F.dense_rank().over(w_starts_session_by_prev_close).alias("prev_closed_at_dense_rank"),
        F.ntile(10).over(w_all_by_closed_at).alias("closed_at_decile"),
        F.round(F.percent_rank().over(w_all_by_closed_at), 3).alias("closed_at_percent_rank"),
    )
)

priority_stats = (
    metrics
    .groupBy("priority")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

compared = (
    metrics.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        F.col("avg_requester_id").alias("avg_requester_id_v"),
        F.col("requester_id_minus_avg").alias("requester_id_minus_avg_v"),
        F.col("closed_at_percent_rank").alias("m_closed_at_percent_rank"),
    )
    .unpivot(
        ids=["id"],
        values=["avg_requester_id_v", "requester_id_minus_avg_v", "m_closed_at_percent_rank"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
