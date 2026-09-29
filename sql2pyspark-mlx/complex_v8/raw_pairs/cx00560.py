from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("claim_date") >= F.lit("2024-07-01").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.add_months(F.col("claim_date"), 12).alias("claim_date_plus_12m"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.hour(F.col("reported_at")).alias("reported_at_hour"),
    )
)

without_matches = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isin("Health", "Property")) & (F.col("a.level") > 2), "left_semi")
)

result = (
    without_matches
    .cube("reported_at_hour", "claim_type", "policy_id")
    .agg(
        F.round(F.sum("claimed_amount"), 2).alias("total_claimed_amount"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.max("claim_date").alias("last_claim_date"),
    )
)
