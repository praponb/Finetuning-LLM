from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team = Window.partitionBy("agent_team")

prepared_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

with_prev = prepared_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

combined_details_agent = (
    sessions.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") != "Billing")
    .select("s.*", F.col("su.team").alias("agent_team"))
)

ranked = (
    combined_details_agent
    .withColumn("agent_team_sum_duration_min", F.sum("duration_min").over(w_agent_team))
    .withColumn("event_count_share_of_agent_team", F.round(F.col("event_count") / F.nullif(F.sum("event_count").over(w_agent_team), F.lit(0)), 4))
)

result = (
    ranked
    .groupBy("agent_id", "session_end")
    .pivot("agent_team", ["Tech", "VIP", "Billing", "Onboarding"])
    .agg(F.avg("burst_id").alias("avg_v"), F.sum("burst_id").alias("sum_v"))
)
