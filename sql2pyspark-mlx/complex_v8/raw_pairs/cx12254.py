from pyspark.sql import functions as F

joined_ranges = (
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
        F.sum("b.approved_amount").alias("prior_approved_amount_60d"),
    )
)

summary = (
    joined_ranges
    .groupBy(
        "prior_rows_60d",
        "policy_id",
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
    )
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 3)
)

result = (
    summary
    .rollup("n_rows", "prior_rows_60d")
    .agg(
        F.min("last_claim_date").alias("first_last_claim_date"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("n_rows"), 2).alias("stddev_n_rows"),
    )
)
