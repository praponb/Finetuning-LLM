from pyspark.sql import functions as F

covered = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 30)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_30d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_30d"),
    )
)

per_group = (
    covered
    .groupBy("prior_rows_30d")
    .agg(
        F.sum("prior_rows_30d").alias("total_prior_rows_30d"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("prior_approved_amount_30d", 0.5).alias("median_prior_approved_amount_30d"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    per_group
    .select(
        "prior_rows_30d",
        "n_rows",
        "median_prior_approved_amount_30d",
        "distinct_ids",
        F.abs(F.col("prior_rows_30d") - 4).alias("prior_rows_30d_distance"),
    )
    .orderBy(F.col("n_rows").desc_nulls_first(), "prior_rows_30d_distance")
)
