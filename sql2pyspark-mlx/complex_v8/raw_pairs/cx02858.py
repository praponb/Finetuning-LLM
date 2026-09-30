from pyspark.sql import functions as F

in_window = (
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

per_group = (
    in_window
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        "policy_id",
        "status",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"),
        F.max("claim_date").alias("last_claim_date"),
        F.count(F.when(F.col("product").isNotNull(), 1)).alias("n_product_is_not_null"),
    )
)

enriched = (
    per_group
    .filter(F.col("n_product_is_not_null") < 8)
    .select(
        "*",
        F.least(F.col("row_count"), F.col("n_product_is_not_null")).alias("least_row_count_n_product_is_not_null"),
        F.dayofweek(F.col("last_claim_date")).alias("last_claim_date_dayofweek"),
    )
)

labeled = (
    enriched
    .select(
        "claim_date_month",
        "policy_id",
        "status",
        "n_product_is_not_null",
        "least_row_count_n_product_is_not_null",
        "last_claim_date_dayofweek",
        (F.floor(F.col("n_product_is_not_null") / 2) * 2).alias("n_product_is_not_null_bucket"),
        (F.col("row_count") + F.col("median_claimed_amount")).alias("row_count_plus_median_claimed_amount"),
    )
)

result = (
    labeled
    .groupBy("least_row_count_n_product_is_not_null")
    .pivot("status", ["IN_REVIEW", "APPROVED", "OPEN"])
    .agg(F.max("row_count_plus_median_claimed_amount"))
)
