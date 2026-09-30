from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_agent_id_by_requester_i = Window.partitionBy("csat", "agent_id").orderBy("requester_id_distance", "id")
w_opened_at_month_agent_team = Window.partitionBy(F.month("opened_at"), "agent_team")
w_all_by_opened_at_full = Window.orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tickets
    .filter(F.col("requester_id") >= 7)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.abs(F.col("requester_id") - 17).alias("requester_id_distance"),
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_activity = (
    combined_details.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.array_contains(F.col("t.tags"), "bug")), "left_anti")
    .filter(F.col("agent_id") >= 8)
)

tagged = (
    with_activity
    .filter(~F.col("agent_team").isin("Onboarding", "Billing"))
    .select(
        "*",
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
        F.size(F.col("tags")).alias("tags_count"),
        F.initcap(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
    )
)

agent_tier_stats = (
    tagged
    .groupBy("agent_tier")
    .agg(F.max("csat").alias("max_csat"))
)

vs_group = (
    tagged.alias("s")
    .join(agent_tier_stats.alias("g"), F.col("g.agent_tier") == F.col("s.agent_tier"), "inner")
    .filter(F.col("s.csat") == F.col("g.max_csat"))
    .select(
        "s.*",
        "g.max_csat",
        F.round(F.col("s.csat") - F.col("g.max_csat"), 2).alias("csat_minus_max"),
    )
)

metrics = (
    vs_group
    .select(
        "*",
        F.dense_rank().over(w_csat_agent_id_by_requester_i).alias("requester_id_distance_dense_rank"),
        F.count(F.lit(1)).over(w_opened_at_month_agent_team).alias("rows_in_opened_at_month_agent_team"),
        F.first("csat").over(w_all_by_opened_at_full).alias("first_csat"),
    )
)

joined = (
    metrics.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.agent_name",
        "s.agent_team",
        "s.agent_tier",
        "s.tags_count",
        "s.agent_name_clean",
        "s.max_csat",
        "s.csat_minus_max",
        "s.requester_id_distance_dense_rank",
        "s.rows_in_opened_at_month_agent_team",
        "s.first_csat",
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

result = (
    joined
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "agent_name",
        "agent_team",
        "agent_tier",
        "agent_name_clean",
        "max_csat",
        "requester_id_distance_dense_rank",
        "rows_in_opened_at_month_agent_team",
        "agent_agent_name",
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
    )
    .orderBy("opened_at")
)
