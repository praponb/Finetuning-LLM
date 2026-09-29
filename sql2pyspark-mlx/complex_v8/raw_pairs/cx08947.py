from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = (
    df_tickets
    .filter(F.col("agent_id") >= 9)
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
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

with_prev = valid_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "session_no")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

joined = (
    sessions.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.upper(F.trim(F.col("su.agent_name"))).like("J%"))
    .select(
        "s.agent_id",
        "s.session_no",
        "s.events",
        "s.duration_min",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

enriched_agent = (
    joined.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.agent_id",
        "s.session_no",
        "s.agent_tier",
        F.col("su.tier").alias("agent_tier_2"),
        F.col("su.agent_name").alias("agent_agent_name"),
        F.col("su.team").alias("agent_team_2"),
    )
)

result = (
    enriched_agent
    .select(
        "agent_id",
        "session_no",
        "agent_tier",
        "agent_tier_2",
        "agent_agent_name",
        F.coalesce(F.col("session_no"), F.lit(0)).alias("session_no_filled"),
    )
    .orderBy("agent_tier", F.col("agent_tier_2").desc_nulls_first())
)
