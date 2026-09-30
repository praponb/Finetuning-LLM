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
        "c.adjuster_id",
        "c.status",
        "p.product",
        "p.premium",
    )
)

enriched = (
    matched
    .filter(F.col("adjuster_id") <= 7)
    .select(
        "*",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
        F.greatest(F.col("premium"), F.col("claimed_amount")).alias("greatest_premium_claimed_amount"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.concat(F.col("status"), F.lit("#"), F.col("premium_bucket").cast("string")).alias("status_ref"),
        F.least(F.col("premium_bucket"), F.col("premium")).alias("least_premium_bucket_premium"),
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
    )
)

result = (
    scored
    .rollup("status", "adjuster_id", "policy_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "adjuster_id",
        "policy_id",
        "n_rows",
        "stddev_premium",
    )
)
