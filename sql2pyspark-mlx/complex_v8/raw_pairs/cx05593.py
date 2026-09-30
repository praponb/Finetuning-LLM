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
        "c.approved_amount",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

totals = (
    in_window
    .groupBy("product", "policy_id", "adjuster_id")
    .agg(
        F.sum("premium").alias("total_premium"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
        F.round(100 * F.count(F.when(F.col("product") == "HOME", 1)) / F.count("*"), 1).alias("pct_product_home"),
    )
)

result = (
    totals
    .groupBy("row_count", "adjuster_id")
    .pivot("product", ["AUTO", "LIFE", "HOME"])
    .agg(F.min("total_premium"))
)
