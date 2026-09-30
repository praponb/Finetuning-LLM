from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_team_in_onboarding_billing = Window.partitionBy("n_team_in_onboarding_billing_t")
w_all_by_distinct_teams = Window.orderBy("distinct_teams", "tier")

cleaned = (
    df_support_agents
    .filter((F.col("team") != "Billing") & (F.year(F.col("hired_on")) == 2024))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
    )
)

summary = (
    cleaned
    .groupBy("tier")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("team").isin("Onboarding", "Billing", "Tech"), 1)).alias("n_team_in_onboarding_billing_t"),
        F.countDistinct("team").alias("distinct_teams"),
        F.datediff(F.max("hired_on"), F.min("hired_on")).alias("hired_on_span_days"),
    )
)

with_trends = (
    summary
    .select(
        "tier",
        "distinct_teams",
        "hired_on_span_days",
        F.sum("cnt").over(w_n_team_in_onboarding_billing).alias("n_team_in_onboarding_billing_t_sum_cnt"),
        F.row_number().over(w_all_by_distinct_teams).alias("seq_no"),
    )
)

tagged = (
    with_trends
    .select(
        "tier",
        "n_team_in_onboarding_billing_t_sum_cnt",
        "seq_no",
        F.when(F.col("tier").isin("T3", "T2"), "group_a").when(F.col("tier") == "T1", "group_b").otherwise("other").alias("tier_group"),
        F.round(F.col("seq_no") / F.nullif(F.col("distinct_teams"), F.lit(0)), 3).alias("seq_no_per_distinct_teams"),
        F.coalesce(F.col("hired_on_span_days"), F.lit(0)).alias("hired_on_span_days_filled"),
    )
)

tier_group_stats = (
    tagged
    .groupBy("tier_group")
    .agg(F.avg("n_team_in_onboarding_billing_t_sum_cnt").alias("avg_n_team_in_onboarding_billing_t_sum_cnt"))
)

compared = (
    tagged.alias("s")
    .join(tier_group_stats.alias("g"), F.col("g.tier_group") == F.col("s.tier_group"), "inner")
    .filter(F.col("s.n_team_in_onboarding_billing_t_sum_cnt") > F.col("g.avg_n_team_in_onboarding_billing_t_sum_cnt"))
    .select(
        "s.*",
        "g.avg_n_team_in_onboarding_billing_t_sum_cnt",
        F.round(F.col("s.n_team_in_onboarding_billing_t_sum_cnt") - F.col("g.avg_n_team_in_onboarding_billing_t_sum_cnt"), 2).alias("n_team_in_onboarding_billing_t_sum_cnt_minus_avg"),
    )
)

result = (
    compared
    .filter(F.col("n_team_in_onboarding_billing_t_sum_cnt_minus_avg").between(-36.0, 48.0))
    .select(
        "tier",
        "n_team_in_onboarding_billing_t_sum_cnt",
        "tier_group",
        "seq_no_per_distinct_teams",
        "hired_on_span_days_filled",
        "avg_n_team_in_onboarding_billing_t_sum_cnt",
        F.lower(F.regexp_replace(F.trim(F.col("tier")), " ", "_")).alias("tier_slug"),
    )
    .orderBy("seq_no_per_distinct_teams", "tier")
    .limit(5)
)
