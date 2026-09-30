from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("closed_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tickets
    .filter(F.col("channel") == "phone")
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

with_prev = valid.withColumn("prev_closed_at", F.lag("closed_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_channel_running))

session_summary = (
    sessionized
    .groupBy("channel", "burst_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

totals = (
    session_summary
    .filter(F.col("event_count").between(2, 6))
    .groupBy(
        "channel",
        F.date_trunc("month", "session_start").alias("session_start_month"),
    )
    .agg(
        F.min("session_end").alias("first_session_end"),
        F.sum(F.when(~F.col("channel").isin("email", "chat"), F.col("burst_id")).otherwise(0)).alias("burst_id_channel_not_in_email_chat"),
        F.round(100 * F.count(F.when(F.col("channel").isin("phone", "chat", "email"), 1)) / F.count("*"), 1).alias("pct_channel_in_phone_chat_email"),
        F.percentile_approx("burst_id", 0.5).alias("median_burst_id"),
        F.round(F.sum("event_count") / F.nullif(F.sum("burst_id"), F.lit(0)), 3).alias("event_count_to_burst_id_ratio"),
        F.sum(F.when(~F.col("channel").isin("web", "email"), 1).otherwise(0)).alias("n_channel_not_in_web_email"),
    )
)

with_activity = (
    totals
    .crossJoin(totals.agg(F.avg("n_channel_not_in_web_email").alias("overall_avg_n_channel_not_in_web_email")))
    .filter(F.col("n_channel_not_in_web_email") > F.col("overall_avg_n_channel_not_in_web_email"))
    .drop("overall_avg_n_channel_not_in_web_email")
)

result = (
    with_activity
    .select(
        "channel",
        "session_start_month",
        "first_session_end",
        "burst_id_channel_not_in_email_chat",
        "pct_channel_in_phone_chat_email",
        "median_burst_id",
        "event_count_to_burst_id_ratio",
        "n_channel_not_in_web_email",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_start_month")).alias("days_since_session_start_month"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
    .orderBy("median_burst_id")
)
