from pyspark.sql import functions as F

in_window = (
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
        F.sum("b.approved_amount").alias("prior_approved_amount_90d"),
    )
)

scored = in_window.withColumn("claim_date_label", F.date_format(F.col("claim_date"), "MMM yyyy"))

result = (
    scored
    .filter(F.col("claim_date_label").isNotNull())
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_90d",
        "prior_approved_amount_90d",
    )
    .orderBy(F.col("claim_date").desc())
)
