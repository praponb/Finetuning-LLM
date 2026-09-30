from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_opened_at_to_date_b = Window.partitionBy("priority", F.to_date("opened_at")).orderBy(F.col("opened_at").desc(), "id")
w_priority_opened_at_to_date_b_2 = Window.partitionBy("priority", F.to_date("opened_at")).orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_priority_opened_at_to_date_b_3 = Window.partitionBy("priority", F.to_date("opened_at")).orderBy(F.col("opened_at").desc(), "id").rowsBetween(-6, -1)

prepared = (
    df_tickets
    .filter(F.col("requester_id") < 16)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
    )
)

qualified = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").between(F.lit("2024-04-14").cast("date"), F.lit("2025-02-27").cast("date"))) & (F.col("su.team").isin("VIP", "Onboarding")), "left_semi")
)

with_trends = (
    qualified
    .select(
        "*",
        F.round(F.cume_dist().over(w_priority_opened_at_to_date_b), 3).alias("opened_at_cume_dist"),
        F.last("csat").over(w_priority_opened_at_to_date_b_2).alias("last_csat"),
        F.lead("csat", 3).over(w_agent_id_by_opened_at).alias("next3_csat"),
        F.max("csat").over(w_priority_agent_id).alias("priority_agent_id_max_csat"),
        F.max("csat").over(w_priority_opened_at_to_date_b_3).alias("csat_max_6_prior"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"), F.count("*").alias("event_count_2"))
)

with_counts = (
    with_trends.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "opened_at_cume_dist",
        "last_csat",
        "next3_csat",
        "priority_agent_id_max_csat",
        "csat_max_6_prior",
        "event_count_2",
        F.concat(F.col("channel"), F.lit("#"), F.col("event_count").cast("string")).alias("channel_ref"),
    )
)
