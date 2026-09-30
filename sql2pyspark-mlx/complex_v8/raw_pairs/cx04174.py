from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_csat_full = Window.partitionBy("priority").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_opened_at = Window.orderBy(F.col("days_since_opened_at").desc(), "id")
w_priority = Window.partitionBy("priority")

recent = (
    df_tickets
    .filter(F.col("channel") != "phone")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.first("closed_at").over(w_priority_by_csat_full).alias("first_closed_at"),
        F.lead("requester_id").over(w_all_by_days_since_opened_at).alias("next_requester_id"),
        F.round(F.col("csat") - F.avg("csat").over(w_priority), 2).alias("csat_vs_priority_avg"),
        F.lead("days_since_opened_at").over(w_all_by_days_since_opened_at).alias("next_days_since_opened_at"),
    )
)

annotated = (
    metrics
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "days_since_opened_at",
        "next_requester_id",
        "csat_vs_priority_avg",
        "next_days_since_opened_at",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.when(F.col("next_requester_id") >= 16, "HIGH").when(F.col("next_requester_id") >= 7, "MEDIUM").otherwise("LOW").alias("next_requester_id_tier"),
        F.round((F.unix_timestamp(F.col("first_closed_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_first_closed_at"),
    )
)

joined_agent = (
    annotated.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.days_since_opened_at",
        "s.next_requester_id",
        "s.csat_vs_priority_avg",
        "s.minutes_closed_at_to_first_closed_at",
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    joined_agent
    .groupingSets(
        [["agent_id", "channel"], ["agent_id"], []],
        "agent_id", "channel",
    )
    .agg(
        F.grouping("agent_id").alias("is_total_agent_id"),
        F.min("closed_at").alias("first_closed_at"),
        F.round(100 * F.count(F.when(F.col("channel") == "phone", 1)) / F.count("*"), 1).alias("pct_channel_phone"),
        F.percentile_approx("minutes_closed_at_to_first_closed_at", 0.5).alias("median_minutes_closed_at_to_first_closed_at"),
    )
)
