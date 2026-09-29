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
        "c.approved_amount",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

qualified = (
    matched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id").isNotNull()) & (F.col("c.claim_date").between(F.lit("2024-03-16").cast("date"), F.lit("2025-11-08").cast("date"))), "left_semi")
)

result = (
    qualified
    .cube("product", "policy_id")
    .agg(
        F.count(F.when(F.col("product") != "TRAVEL", 1)).alias("n_product_travel"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "policy_id",
        "n_product_travel",
        "row_count",
    )
    .orderBy(F.col("product").asc_nulls_last(), F.col("policy_id").asc_nulls_last())
)
