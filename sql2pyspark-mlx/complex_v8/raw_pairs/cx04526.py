from pyspark.sql import functions as F

recent = (
    df_adjusters
    .filter(F.col("adjuster_name").isNotNull())
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.when(F.col("team").isin("Travel", "Property"), "group_a").when(F.col("team") == "Motor", "group_b").otherwise("other").alias("team_group"),
        F.abs(F.col("level") - 3).alias("level_distance"),
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
    )
)

detail_rows_claims = (
    recent.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.team",
        "s.team_group",
        "s.level_distance",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.policy_id").alias("claim_policy_id"),
        "c.claim_type",
    )
)

combined_details_policy = (
    detail_rows_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.claim_policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.team",
        "s.claim_id",
        "s.claim_claimed_amount",
        "s.claim_approved_amount",
        "s.claim_policy_id",
        "s.claim_type",
        F.col("p.start_date").alias("policy_start_date"),
        "po.holder_name",
    )
)

qualified = (
    combined_details_policy
    .crossJoin(combined_details_policy.agg(F.max("claim_claimed_amount").alias("overall_max_claim_claimed_amount")))
    .filter(F.col("claim_claimed_amount") < F.col("overall_max_claim_claimed_amount"))
    .drop("overall_max_claim_claimed_amount")
)

grouped = (
    qualified
    .filter(F.col("claim_approved_amount") > 7204.1)
    .groupBy(
        F.year("policy_start_date").alias("policy_start_date_year"),
        "claim_id",
        "team",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("claim_claimed_amount"), 2).alias("avg_claim_claimed_amount"),
        F.datediff(F.max("policy_start_date"), F.min("policy_start_date")).alias("policy_start_date_span_days"),
        F.array_sort(F.collect_set("holder_name")).alias("holder_name_list"),
        F.array_sort(F.collect_set("team")).alias("team_list"),
        F.percentile_approx("claim_approved_amount", 0.5).alias("median_claim_approved_amount"),
        F.max("claim_approved_amount").alias("max_claim_approved_amount_check"),
    )
    .filter(F.col("max_claim_approved_amount_check") > 0.0)
    .drop("max_claim_approved_amount_check")
)

result = (
    grouped
    .select(
        "policy_start_date_year",
        "claim_id",
        "median_claim_approved_amount",
        "avg_claim_claimed_amount",
    )
    .unpivot(
        ids=["policy_start_date_year", "claim_id"],
        values=["median_claim_approved_amount", "avg_claim_claimed_amount"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("policy_start_date_year", "claim_id", "field")
)
