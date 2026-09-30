from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = df_tickets

with_prev = valid.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_channel_running))

per_session = (
    sessionized
    .groupBy("channel", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    per_session
    .filter(F.hour(F.col("session_end")).between(9, 12))
    .select(
        "channel",
        "burst_id",
        "session_start",
        "events",
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
        F.round(F.col("duration_min") / F.nullif(F.col("events"), F.lit(0)), 3).alias("duration_min_per_events"),
        F.hour(F.col("session_end")).alias("session_end_hour"),
    )
)

qualified = (
    annotated
    .crossJoin(annotated.agg(F.avg("session_end_hour").alias("overall_avg_session_end_hour")))
    .filter(F.col("session_end_hour") > F.col("overall_avg_session_end_hour"))
    .drop("overall_avg_session_end_hour")
)

result = (
    qualified
    .groupBy("session_end_hour")
    .pivot("channel", ["email", "phone", "chat"])
    .agg(F.sum("events"))
    .orderBy("session_end_hour")
)
