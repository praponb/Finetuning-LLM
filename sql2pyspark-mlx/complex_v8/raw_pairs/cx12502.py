from pyspark.sql import functions as F

in_window = (
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
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

rollup_base = (
    in_window
    .filter(F.col("claimed_amount") <= 33690.0)
    .groupBy("product", "claim_type")
    .agg(
        F.round(100 * F.count(F.when(F.col("product") == "HOME", 1)) / F.count("*"), 1).alias("pct_product_home"),
        F.count("*").alias("cnt"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.max("approved_amount").alias("max_approved_amount_check"),
    )
    .filter(F.col("max_approved_amount_check") > 2569.2)
    .drop("max_approved_amount_check")
)

result = (
    rollup_base
    .select(
        "product",
        "pct_product_home",
        "cnt",
        "distinct_policy_ids",
        "total_claimed_amount",
    )
    .orderBy(F.col("pct_product_home").desc(), "total_claimed_amount")
)
