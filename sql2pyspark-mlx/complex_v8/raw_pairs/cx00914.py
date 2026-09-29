from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter(~F.col("status").isin("APPROVED", "OPEN"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.date_add(F.col("claim_date"), 365).alias("claim_date_plus_365d"),
    )
)

tagged = (
    prepared_claims
    .filter(F.col("claim_date_plus_365d") >= F.lit("2024-07-01").cast("date"))
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "claim_date_plus_365d",
        F.datediff(F.col("claim_date"), F.col("claim_date_plus_365d")).alias("days_claim_date_plus_365d_to_claim_date"),
    )
)

tagged_2 = (
    tagged
    .withColumn("claimed_amount_tier", F.when(F.col("claimed_amount") >= 31715.7, "HIGH").when(F.col("claimed_amount") >= 10518.4, "MEDIUM").otherwise("LOW"))
)

result = (
    tagged_2
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "claim_date_plus_365d",
        "claimed_amount_tier",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)
