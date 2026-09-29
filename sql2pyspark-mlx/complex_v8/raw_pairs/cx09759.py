from pyspark.sql import functions as F

in_window = (
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

derived = (
    in_window
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_30d",
        "prior_approved_amount_30d",
        F.abs(F.col("prior_rows_30d") - 4).alias("prior_rows_30d_distance"),
        F.abs(F.col("prior_rows_30d") - 4).alias("prior_rows_30d_distance_2"),
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
    )
)

totals = (
    derived
    .groupBy("prior_rows_30d_distance")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.min("claim_date").alias("first_claim_date"),
        F.round(F.sum("prior_rows_30d_distance") / F.nullif(F.sum("prior_rows_30d"), F.lit(0)), 3).alias("prior_rows_30d_distance_to_prior_rows_30d_ratio"),
        F.datediff(F.max("claim_date_month_end"), F.min("claim_date_month_end")).alias("claim_date_month_end_span_days"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
    )
)

result = (
    totals
    .select(
        "prior_rows_30d_distance",
        "last_claim_date",
        "first_claim_date",
        "prior_rows_30d_distance_to_prior_rows_30d_ratio",
        "claim_date_month_end_span_days",
        "distinct_policy_ids",
    )
    .orderBy(
        F.col("prior_rows_30d_distance_to_prior_rows_30d_ratio").desc(),
        F.col("first_claim_date").desc(),
    )
)
