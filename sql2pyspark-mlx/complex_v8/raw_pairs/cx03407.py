from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

enriched = (
    cleaned_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "claim_type_len",
        "status_claim_type_label",
        F.add_months(F.col("claim_date"), 12).alias("claim_date_plus_12m"),
        F.date_format(F.col("reported_at"), "yyyy-MM-dd").alias("reported_at_label"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

combined_details_policy = (
    enriched.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.claim_type_len",
        "s.status_claim_type_label",
        "s.reported_at_label",
        "s.approved_amount_bucket",
        F.col("p.product").alias("policy_product"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

claim_type_stats = (
    combined_details_policy
    .groupBy("claim_type")
    .agg(F.avg("claim_type_len").alias("avg_claim_type_len"))
)

outliers = (
    combined_details_policy.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.claim_type_len") > F.col("g.avg_claim_type_len"))
    .select(
        "s.*",
        "g.avg_claim_type_len",
        F.round(F.col("s.claim_type_len") - F.col("g.avg_claim_type_len"), 2).alias("claim_type_len_minus_avg"),
    )
)

result = (
    outliers
    .groupingSets(
        [["policy_product", "adjuster_team"], ["policy_product"], ["adjuster_team"], []],
        "policy_product", "adjuster_team",
    )
    .agg(
        F.grouping("policy_product").alias("is_total_policy_product"),
        F.count("*").alias("cnt"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
    )
    .select(
        F.coalesce(F.col("policy_product"), F.lit("ALL")).alias("policy_product"),
        F.coalesce(F.col("adjuster_team"), F.lit("ALL")).alias("adjuster_team"),
        "is_total_policy_product",
        "cnt",
        "avg_claimed_amount",
    )
)
