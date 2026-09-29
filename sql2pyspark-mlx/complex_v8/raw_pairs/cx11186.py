from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tickets
    .filter(F.col("requester_id") < 22)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
    )
)

with_prev = cleaned.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_channel_running))

sessions = (
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

result = (
    sessions
    .filter(F.col("session_requester_id") >= 26)
    .select(
        "channel",
        "session_no",
        "session_end",
        "events",
        "session_requester_id",
        "duration_min",
        F.when(F.col("channel").isin("chat", "web"), "group_a").when(F.col("channel") == "email", "group_b").otherwise("other").alias("channel_group"),
    )
    .orderBy(F.col("session_no").asc(), "channel")
    .limit(5)
)
