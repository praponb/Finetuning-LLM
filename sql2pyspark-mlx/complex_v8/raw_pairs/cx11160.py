from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_hired_on_2_year_n_agen = Window.partitionBy("agent_hired_on_2_year", "n_agent_team_2_in_vip_tech").orderBy("distinct_agent_team_2s", "agent_team_2", "agent_hired_on_2_year")
w_agent_team_2 = Window.partitionBy("agent_team_2")

base_tickets = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")) & (F.col("agent_id").isNotNull()))
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
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd HH:00").alias("closed_at_label"),
        F.concat_ws(" / ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

enriched_agent = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.closed_at_label",
        "s.priority_channel_label",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

derived = (
    enriched_agent
    .filter(F.col("agent_hired_on") < F.lit("2025-02-27").cast("date"))
    .select(
        "*",
        F.upper(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
    )
)

combined_details = (
    derived.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") != "Tech")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.closed_at_label",
        "s.agent_hired_on",
        "s.agent_team",
        "s.greatest_requester_id_csat",
        F.col("su.hired_on").alias("agent_hired_on_2"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team_2"),
    )
)

per_group = (
    combined_details
    .groupBy(
        "agent_team_2",
        F.year("agent_hired_on_2").alias("agent_hired_on_2_year"),
    )
    .agg(
        F.countDistinct("agent_team_2").alias("distinct_agent_team_2s"),
        F.max("agent_hired_on").alias("last_agent_hired_on"),
        F.count(F.when(F.col("agent_team_2").isin("VIP", "Tech"), 1)).alias("n_agent_team_2_in_vip_tech"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.round(100 * F.count(F.when(F.col("agent_team_2") == "Billing", 1)) / F.count("*"), 1).alias("pct_agent_team_2_billing"),
        F.max("greatest_requester_id_csat").alias("max_greatest_requester_id_csat_check"),
    )
    .filter(F.col("max_greatest_requester_id_csat_check") > 6)
    .drop("max_greatest_requester_id_csat_check")
)

ranked = (
    per_group
    .select(
        "*",
        (F.col("agent_hired_on_2_year") - F.lag("agent_hired_on_2_year").over(w_agent_hired_on_2_year_n_agen)).alias("agent_hired_on_2_year_change"),
        F.round(F.col("pct_agent_team_2_billing") - F.avg("pct_agent_team_2_billing").over(w_agent_team_2), 2).alias("pct_agent_team_2_billing_vs_agent_team_2_avg"),
        F.round(F.col("n_agent_team_2_in_vip_tech") / F.nullif(F.sum("n_agent_team_2_in_vip_tech").over(w_agent_team_2), F.lit(0)), 4).alias("n_agent_team_2_in_vip_tech_share_of_agent_team_2"),
        F.ntile(5).over(w_agent_hired_on_2_year_n_agen).alias("distinct_agent_team_2s_quintile"),
    )
)

derived_2 = (
    ranked
    .select(
        "*",
        F.date_format(F.col("last_agent_hired_on"), "MMM yyyy").alias("last_agent_hired_on_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_agent_hired_on")).alias("days_since_last_agent_hired_on"),
    )
)

result = (
    derived_2
    .select(
        "agent_team_2",
        "distinct_agent_team_2s",
        "last_agent_hired_on",
        "n_agent_team_2_in_vip_tech",
        "distinct_agent_ids",
        "pct_agent_team_2_billing",
        "pct_agent_team_2_billing_vs_agent_team_2_avg",
        "n_agent_team_2_in_vip_tech_share_of_agent_team_2",
        "distinct_agent_team_2s_quintile",
    )
    .orderBy(F.col("pct_agent_team_2_billing").asc(), "distinct_agent_ids")
)
