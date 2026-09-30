from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = df_tickets.filter(F.array_contains(F.col("tags"), "login"))

with_prev = base_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_channel_running))

per_session = (
    sessionized
    .groupBy("channel", "session_no")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    per_session
    .select(
        "*",
        (F.floor(F.col("session_requester_id") / 25) * 25).alias("session_requester_id_bucket"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

screened = (
    derived
    .crossJoin(derived.agg(F.max("duration_min").alias("overall_max_duration_min")))
    .filter(F.col("duration_min") < F.col("overall_max_duration_min"))
    .drop("overall_max_duration_min")
)

result = (
    screened
    .select(
        "channel",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        "session_requester_id_bucket",
        F.abs(F.col("duration_min") - 108.0).alias("duration_min_distance"),
        F.year(F.col("session_end")).alias("session_end_year"),
    )
    .orderBy("duration_min_distance")
)
