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
        "c.adjuster_id",
        "c.status",
        "p.product",
        "p.premium",
    )
)

totals = (
    in_window
    .groupBy("policy_id", "status", "adjuster_id")
    .agg(
        F.max("claimed_amount").alias("max_claimed_amount"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.round(F.sum("premium") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("premium_to_claimed_amount_ratio"),
        F.countDistinct("status").alias("distinct_status"),
        F.sum(F.when(F.col("product") == "TRAVEL", 1).otherwise(0)).alias("n_product_travel"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
    )
)

result = (
    totals
    .select(
        "policy_id",
        "status",
        "max_claimed_amount",
        "distinct_policy_ids",
        "premium_to_claimed_amount_ratio",
        "distinct_status",
        "n_product_travel",
        "stddev_premium",
    )
    .orderBy(F.col("status").desc(), F.col("premium_to_claimed_amount_ratio").desc())
)
