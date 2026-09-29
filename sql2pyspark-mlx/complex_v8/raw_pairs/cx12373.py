from pyspark.sql import functions as F

joined_ranges = (
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
        "c.claim_type",
        "c.status",
        "p.product",
        "p.premium",
    )
)

rollup_base = (
    joined_ranges
    .filter(~F.col("status").isin("APPROVED", "OPEN"))
    .groupBy("product", "claim_type", "status")
    .agg(
        F.count("*").alias("n_rows"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.round(100 * F.count(F.when(F.col("product") == "AUTO", 1)) / F.count("*"), 1).alias("pct_product_auto"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    rollup_base
    .filter(F.col("status").isNotNull())
    .select(
        "product",
        "claim_type",
        "status",
        "n_rows",
        "claim_date_span_days",
        "pct_product_auto",
        (F.floor(F.col("pct_product_auto") / 25) * 25).alias("pct_product_auto_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
    .orderBy(F.col("product").asc())
)
