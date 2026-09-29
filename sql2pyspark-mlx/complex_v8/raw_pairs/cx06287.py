from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("closed_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
    )
)

with_prev = cleaned.withColumn("prev_closed_at", F.lag("closed_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_channel_running))

per_session = (
    sessionized
    .groupBy("channel", "session_no")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

screened = (
    per_session
    .crossJoin(per_session.agg(F.avg("session_csat").alias("overall_avg_session_csat")))
    .filter(F.col("session_csat") > F.col("overall_avg_session_csat"))
    .drop("overall_avg_session_csat")
)

result = (
    screened
    .select(
        "channel",
        "session_no",
        "session_end",
        "events",
        "session_csat",
        "duration_min",
    )
    .orderBy("session_no", F.col("events").desc_nulls_first())
)
