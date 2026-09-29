from pyspark.sql import functions as F

matched = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

derived = (
    matched
    .filter(F.col("product").isNotNull())
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "claim_type",
        "product",
        "premium",
        F.round(F.col("claimed_amount") * 0.25, 2).alias("claimed_amount_adj"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

combined_details = (
    derived.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.start_date") >= F.lit("2025-01-05").cast("date"))
    .select("s.*", F.col("p.start_date").alias("policy_start_date"))
)

product_stats = (
    combined_details
    .groupBy("product")
    .agg(F.percentile_approx("product_len", 0.5).alias("median_product_len"))
)

outliers = (
    combined_details.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.product_len") >= F.col("g.median_product_len"))
    .select(
        "s.*",
        "g.median_product_len",
        F.round(F.col("s.product_len") - F.col("g.median_product_len"), 2).alias("product_len_minus_median"),
    )
)

result = (
    outliers
    .filter(F.col("claimed_amount_adj") > 30060.0)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "product",
        "premium",
        "claimed_amount_adj",
        "product_len",
        "median_product_len",
        "product_len_minus_median",
        F.round(F.col("product_len") / F.nullif(F.col("median_product_len"), F.lit(0)), 3).alias("product_len_per_median_product_len"),
        F.when(F.col("status").isin("REJECTED", "IN_REVIEW"), "group_a").when(F.col("status") == "APPROVED", "group_b").otherwise("other").alias("status_group"),
    )
    .orderBy("status_group")
)
