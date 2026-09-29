from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_all_by_requester_id_moving_6 = Window.orderBy("requester_id", "id").rowsBetween(-6, Window.currentRow)
w_burst_id = Window.partitionBy("burst_id")

scoped = (
    df_tickets
    .filter(F.col("csat") >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

with_prev = scoped.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_agent_id_running))

with_windows = (
    sessionized
    .select(
        "*",
        F.row_number().over(w_all_by_requester_id).alias("rn"),
        F.sum("requester_id").over(w_all_by_requester_id_moving_6).alias("requester_id_sum_7"),
        F.round(F.col("starts_session") - F.avg("starts_session").over(w_burst_id), 2).alias("starts_session_vs_burst_id_avg"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
        "channel_or_unknown",
        "prev_closed_at",
        "starts_session",
        "burst_id",
        "starts_session_vs_burst_id_avg",
        F.coalesce(F.col("starts_session"), F.lit(0)).alias("starts_session_filled"),
        F.round((F.unix_timestamp(F.col("prev_closed_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_prev_closed_at"),
    )
)
