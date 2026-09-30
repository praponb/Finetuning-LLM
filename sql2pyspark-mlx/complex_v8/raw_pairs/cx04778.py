from pyspark.sql import functions as F

in_window = (
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

scored = in_window.select("*", F.date_add(F.col("claim_date"), 90).alias("claim_date_plus_90d"))

result = (
    scored
    .filter(F.col("claim_date").between(F.lit("2024-03-30").cast("date"), F.lit("2025-04-25").cast("date")))
    .select(
        "id",
        "prior_rows_60d",
        "claim_date_plus_90d",
        F.abs(F.col("prior_rows_60d") - 4).alias("prior_rows_60d_distance"),
        F.coalesce(F.col("prior_rows_60d"), F.lit(0)).alias("prior_rows_60d_filled"),
    )
    .orderBy("claim_date_plus_90d", F.col("id").asc_nulls_last())
)
