from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

with_prev = prepared.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_channel_running))

session_summary = (
    sessionized
    .groupBy("channel", "session_no")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    session_summary
    .withColumn("session_end_label", F.date_format(F.col("session_end"), "yyyy-MM-dd"))
)

result = (
    derived
    .filter(~F.col("channel").isin("web", "chat"))
    .select(
        "channel",
        "session_start",
        "event_count",
        "session_csat",
        "session_end_label",
    )
    .orderBy("session_end_label", "session_csat")
)
