from pyspark.sql import functions as F

matched = (
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

totals = (
    matched
    .groupBy("prior_rows_30d")
    .agg(
        F.max("prior_approved_amount_30d").alias("max_prior_approved_amount_30d"),
        F.sum("prior_rows_30d").alias("total_prior_rows_30d"),
    )
)

result = (
    totals
    .filter(F.col("total_prior_rows_30d") <= 29)
    .select("prior_rows_30d")
    .orderBy(F.col("prior_rows_30d").asc_nulls_last())
)
