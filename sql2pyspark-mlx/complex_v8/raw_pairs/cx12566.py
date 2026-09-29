from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-02-18 15:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
    )
)

claim_side = cleaned_claims.select("claim_type", F.col("claim_date").alias("day"))

claim_side_2 = df_claims.select("claim_type", "claim_date")

unioned = claim_side.union(claim_side_2)

result = (
    unioned
    .groupBy("claim_type")
    .agg(F.count("*").alias("n_rows"), F.max("day").alias("latest_day"))
    .orderBy("claim_type")
)
