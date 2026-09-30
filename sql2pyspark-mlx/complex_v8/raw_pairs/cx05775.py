from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

with_prev = filtered_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_channel_running))

joined_agent = (
    sessionized.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.agent_name").isNotNull()), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.priority_or_unknown",
        "s.prev_opened_at",
        "s.new_session_flag",
        "s.session_no",
        F.col("su.tier").alias("agent_tier"),
    )
)

tagged = (
    joined_agent
    .select(
        "*",
        F.round(F.col("session_no") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("session_no_per_requester_id"),
        F.date_trunc("year", F.col("prev_opened_at")).alias("prev_opened_at_year"),
        F.date_trunc("year", F.col("prev_opened_at")).alias("prev_opened_at_year_2"),
    )
)

result = (
    tagged
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "prev_opened_at",
        "new_session_flag",
        "session_no",
        "agent_tier",
        "session_no_per_requester_id",
        "prev_opened_at_year",
        "prev_opened_at_year_2",
    )
    .orderBy(F.col("requester_id").desc())
)
