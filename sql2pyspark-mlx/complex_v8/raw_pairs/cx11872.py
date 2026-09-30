from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_no_session_end_year = Window.partitionBy("session_no", F.year("session_end"))
w_session_start_year_session_n = Window.partitionBy(F.year("session_start"), "session_no").orderBy("duration_min", "priority", "session_no")

cleaned = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
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
    )
)

with_prev = cleaned.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_priority_running))

per_session = (
    sessionized
    .groupBy("priority", "session_no")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    per_session
    .select(
        "*",
        F.round(F.col("session_no") / F.nullif(F.sum("session_no").over(w_session_no_session_end_year), F.lit(0)), 4).alias("session_no_share_of_session_no_session_end_year"),
        F.row_number().over(w_session_start_year_session_n).alias("duration_min_seq"),
    )
)

result = (
    with_trends
    .filter(F.col("duration_min") < 180.0)
    .select(
        "priority",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "session_csat",
        "duration_min",
        "session_no_share_of_session_no_session_end_year",
        "duration_min_seq",
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 3600, 1).alias("hours_session_start_to_session_end"),
        F.least(F.col("duration_min_seq"), F.col("session_no")).alias("least_duration_min_seq_session_no"),
    )
    .orderBy("session_no", "hours_session_start_to_session_end")
)
