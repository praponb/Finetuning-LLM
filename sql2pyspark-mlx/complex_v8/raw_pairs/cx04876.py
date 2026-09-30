from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type", "ticket_id").orderBy("ticket_csat_minus_avg", "id")
w_dedup_2 = Window.partitionBy("agent_tier", "actor").orderBy(F.col("agent_hired_on").desc(), F.col("id").desc())

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

combined_details_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

actor_group_stats = (
    combined_details_ticket
    .groupBy("actor_group")
    .agg(F.avg("ticket_csat").alias("avg_ticket_csat"))
)

vs_group = (
    combined_details_ticket.alias("s")
    .join(actor_group_stats.alias("g"), F.col("g.actor_group") == F.col("s.actor_group"), "inner")
    .filter(F.col("s.ticket_csat") > F.col("g.avg_ticket_csat"))
    .select(
        "s.*",
        "g.avg_ticket_csat",
        F.round(F.col("s.ticket_csat") - F.col("g.avg_ticket_csat"), 2).alias("ticket_csat_minus_avg"),
    )
)

latest_per_key = (
    vs_group
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    latest_per_key
    .withColumn("recency_rank", F.row_number().over(w_dedup_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched = (
    latest
    .filter(F.col("event_type_clean").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_group",
        "ticket_requester_id",
        "ticket_csat",
        "ticket_csat_minus_avg",
        F.lower(F.regexp_replace(F.trim(F.col("agent_tier")), " ", "_")).alias("agent_tier_slug"),
        (F.floor(F.col("ticket_csat_minus_avg") / 2) * 2).alias("ticket_csat_minus_avg_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_group",
        "ticket_requester_id",
        "ticket_csat",
        "ticket_csat_minus_avg",
        "agent_tier_slug",
        "ticket_csat_minus_avg_bucket",
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group_2"),
    )
    .orderBy(F.col("agent_tier_slug").asc_nulls_last())
)
