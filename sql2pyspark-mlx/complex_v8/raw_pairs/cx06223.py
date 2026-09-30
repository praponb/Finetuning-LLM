from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.to_date(F.col("reported_at")) < F.lit("2025-02-24").cast("date")) & (F.col("claim_date") < F.lit("2025-04-25").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

derived = (
    valid
    .select(
        "*",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.year(F.col("claim_date")).alias("claim_date_year"),
    )
)

screened = (
    derived.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id") >= 2), "left_semi")
)

tagged = (
    screened
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "status",
        "approved_amount_plus_claimed_amount",
        "status_clean",
        "claimed_amount_filled",
        "claim_date_year",
        F.date_trunc("hour", F.col("reported_at")).alias("reported_at_hour"),
        F.when(F.col("claimed_amount_filled") >= 37525.0, "HIGH").when(F.col("claimed_amount_filled") >= 10080.0, "MEDIUM").otherwise("LOW").alias("claimed_amount_filled_tier"),
        F.round(F.col("claimed_amount_filled") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("claimed_amount_filled_per_claimed_amount"),
    )
)

screened_2 = (
    tagged
    .crossJoin(tagged.agg(F.avg("claimed_amount_filled").alias("overall_avg_claimed_amount_filled")))
    .filter(F.col("claimed_amount_filled") > F.col("overall_avg_claimed_amount_filled"))
    .drop("overall_avg_claimed_amount_filled")
)

result = (
    screened_2
    .select("id", "claimed_amount", "claimed_amount_filled_per_claimed_amount")
    .unpivot(
        ids=["id"],
        values=["claimed_amount", "claimed_amount_filled_per_claimed_amount"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
