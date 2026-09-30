from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_min = Window.orderBy(F.col("duration_min").desc_nulls_first(), "priority", "session_id")
w_session_id_session_end_month = Window.partitionBy("session_id", F.date_trunc("month", "session_end")).orderBy(F.col("session_requester_id").asc_nulls_last(), "priority", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_2 = Window.partitionBy("priority")

scoped_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat_ws("|", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_priority_running))

sessions = (
    sessionized
    .groupBy("priority", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    sessions
    .select(
        "priority",
        "session_id",
        "session_start",
        "session_end",
        "duration_min",
        F.ntile(4).over(w_all_by_duration_min).alias("duration_min_quartile"),
        F.sum("session_id").over(w_session_id_session_end_month).alias("running_session_id"),
        F.sum("session_requester_id").over(w_priority_2).alias("priority_sum_session_requester_id"),
        F.round(F.col("duration_min") / F.nullif(F.sum("duration_min").over(w_priority_2), F.lit(0)), 4).alias("duration_min_share_of_priority"),
    )
)

screened = (
    with_trends
    .crossJoin(with_trends.agg(F.max("running_session_id").alias("overall_max_running_session_id")))
    .filter(F.col("running_session_id") < F.col("overall_max_running_session_id"))
    .drop("overall_max_running_session_id")
)

result = (
    screened
    .select(
        "priority",
        "session_id",
        "session_start",
        "session_end",
        "duration_min",
        "running_session_id",
        "priority_sum_session_requester_id",
        "duration_min_share_of_priority",
    )
    .orderBy("duration_min_share_of_priority", F.col("session_end").asc())
)
