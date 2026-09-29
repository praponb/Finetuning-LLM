from pyspark.sql import functions as F

joined_ranges = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 90)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_90d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_90d"),
    )
)

scored = (
    joined_ranges
    .select(
        "*",
        (F.floor(F.col("prior_claimed_amount_90d") / 50000) * 50000).alias("prior_claimed_amount_90d_bucket"),
        (F.floor(F.col("prior_claimed_amount_90d") / 50000) * 50000).alias("prior_claimed_amount_90d_bucket_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "prior_rows_90d",
        "prior_claimed_amount_90d_bucket",
        "prior_claimed_amount_90d_bucket_2",
    )
    .unpivot(
        ids=["id"],
        values=["prior_rows_90d", "prior_claimed_amount_90d_bucket", "prior_claimed_amount_90d_bucket_2"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric_name")
)
