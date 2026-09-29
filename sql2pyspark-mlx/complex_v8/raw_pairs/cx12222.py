from pyspark.sql import functions as F

matched = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 180)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_180d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_180d"),
    )
)

summary = (
    matched
    .filter(F.col("claim_date").between(F.lit("2024-06-13").cast("date"), F.lit("2025-05-31").cast("date")))
    .groupBy("policy_id", "prior_rows_180d")
    .agg(
        F.round(F.sum("prior_approved_amount_180d") / F.nullif(F.sum("prior_rows_180d"), F.lit(0)), 3).alias("prior_approved_amount_180d_to_prior_rows_180d_ra"),
        F.min("claim_date").alias("first_claim_date"),
        F.min("prior_approved_amount_180d").alias("min_prior_approved_amount_180d"),
        F.round(F.avg("prior_rows_180d"), 2).alias("avg_prior_rows_180d"),
        F.round(F.avg("prior_approved_amount_180d"), 2).alias("avg_prior_approved_amount_180d"),
    )
)

annotated = (
    summary
    .select(
        "*",
        F.date_format(F.col("first_claim_date"), "yyyy-MM").alias("first_claim_date_label"),
        F.date_add(F.col("first_claim_date"), 365).alias("first_claim_date_plus_365d"),
    )
)

result = (
    annotated
    .select(
        "policy_id",
        "prior_rows_180d",
        "prior_approved_amount_180d_to_prior_rows_180d_ra",
        "first_claim_date",
        "min_prior_approved_amount_180d",
        "avg_prior_approved_amount_180d",
        "first_claim_date_label",
        "first_claim_date_plus_365d",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_claim_date")).alias("days_since_first_claim_date"),
    )
    .orderBy(
        F.col("avg_prior_approved_amount_180d").desc(),
        F.col("policy_id").desc(),
    )
)
