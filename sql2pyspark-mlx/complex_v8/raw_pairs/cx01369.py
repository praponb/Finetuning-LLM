from pyspark.sql import functions as F

covered = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 60)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_60d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_60d"),
    )
)

without_matches = (
    covered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date").between(F.lit("2024-03-01").cast("date"), F.lit("2025-04-20").cast("date"))) & (F.col("c.reported_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")), "left_anti")
)

enriched = (
    without_matches.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_60d",
        "s.prior_claimed_amount_60d",
        F.col("p.status").alias("policy_status"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
        "po.holder_name",
    )
)

derived = (
    enriched
    .filter(F.col("prior_rows_60d") < 5)
    .select(
        "*",
        F.abs(F.col("prior_rows_60d") - 5).alias("prior_rows_60d_distance"),
        F.substring(F.trim(F.col("policy_product")), 1, 4).alias("policy_product_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("policy_product")), " ", "_")).alias("policy_product_slug"),
    )
)

result = (
    derived
    .groupBy("policy_status", "policy_id")
    .pivot("policy_product", ["LIFE", "AUTO"])
    .agg(F.min("prior_claimed_amount_60d"))
)
