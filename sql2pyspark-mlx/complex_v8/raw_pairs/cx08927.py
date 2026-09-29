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
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

labeled = (
    matched
    .filter(F.col("approved_amount") > 4868.7)
    .select(
        "*",
        F.date_add(F.col("claim_date"), 365).alias("claim_date_plus_365d"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

grouped = (
    labeled
    .filter(F.col("premium_filled") <= 3800.0)
    .groupBy(F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"))
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.countDistinct("id").alias("distinct_ids"),
        F.datediff(F.max("claim_date_plus_365d"), F.min("claim_date_plus_365d")).alias("claim_date_plus_365d_span_days"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
    )
)

result = (
    grouped
    .select(
        "claim_date_ym",
        "avg_premium",
        "distinct_ids",
        "claim_date_plus_365d_span_days",
        F.coalesce(F.col("avg_premium"), F.lit(0.0)).alias("avg_premium_filled"),
        F.length(F.trim(F.col("claim_date_ym"))).alias("claim_date_ym_len"),
    )
    .orderBy(
        F.col("avg_premium").desc_nulls_first(),
        F.col("claim_date_ym_len").desc_nulls_first(),
    )
)
