from pyspark.sql import functions as F

joined_ranges = (
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

without_matches = (
    joined_ranges.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
)

scored = (
    without_matches
    .filter(F.col("claim_date").isNotNull())
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
        F.abs(F.col("prior_rows_30d") - 3).alias("prior_rows_30d_distance"),
    )
)

result = (
    scored
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_30d",
        "prior_approved_amount_30d",
        "days_since_claim_date",
        "claim_date_month_end",
        "prior_rows_30d_distance",
        F.date_trunc("month", F.col("claim_date_month_end")).alias("claim_date_month_end_month"),
        F.abs(F.col("prior_rows_30d") - 2).alias("prior_rows_30d_distance_2"),
    )
    .orderBy(F.col("claim_date").asc_nulls_last())
)
