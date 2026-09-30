from pyspark.sql import functions as F

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.status",
        "p.product",
        "p.premium",
    )
)

tagged = (
    joined_ranges
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.abs(F.col("claimed_amount") - 32878.5).alias("claimed_amount_distance"),
        F.concat_ws("|", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

joined_policy = (
    tagged.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.premium") < 3544.1)
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.claim_type",
        "s.status",
        "s.product",
        "s.premium",
        "s.status_slug",
        "s.claimed_amount_distance",
        "s.claim_type_status_label",
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
    )
)

policy_product_stats = (
    joined_policy
    .groupBy("policy_product")
    .agg(F.percentile_approx("claimed_amount_distance", 0.5).alias("median_claimed_amount_distance"))
)

compared = (
    joined_policy.alias("s")
    .join(policy_product_stats.alias("g"), F.col("g.policy_product") == F.col("s.policy_product"), "inner")
    .filter(F.col("s.claimed_amount_distance") >= F.col("g.median_claimed_amount_distance"))
    .select(
        "s.*",
        "g.median_claimed_amount_distance",
        F.round(F.col("s.claimed_amount_distance") - F.col("g.median_claimed_amount_distance"), 2).alias("claimed_amount_distance_minus_median"),
    )
)

result = (
    compared
    .filter(F.col("status").isin("PAID", "APPROVED", "IN_REVIEW"))
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "claim_type",
        "status",
        "product",
        "claimed_amount_distance",
        "claim_type_status_label",
        "policy_product",
        "policy_premium",
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
        F.round(F.col("policy_premium") * 0.9, 2).alias("policy_premium_adj"),
    )
)
