from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("closed_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

with_prev = prepared_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_channel_running))

session_summary = (
    sessionized
    .groupBy("channel", "session_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

tagged = (
    session_summary
    .filter(F.col("event_count") >= 3)
    .select("*", F.year(F.col("session_end")).alias("session_end_year"))
)

result = (
    tagged
    .select(
        "channel",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        "session_end_year",
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 60, 1).alias("minutes_session_end_to_session_start"),
        F.round(F.col("duration_min") * 1.1, 2).alias("duration_min_adj"),
    )
    .orderBy(
        F.col("minutes_session_end_to_session_start").desc_nulls_first(),
        "duration_min_adj",
    )
)
