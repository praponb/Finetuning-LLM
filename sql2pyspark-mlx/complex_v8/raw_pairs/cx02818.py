from pyspark.sql import functions as F

adjuster_side = (
    df_adjusters
    .select(
        F.col("id").alias("ref_id"),
        F.col("level").alias("key_id"),
        F.lit("adjuster").alias("src"),
    )
)

claim_side = (
    df_claims
    .filter(F.col("claimed_amount") >= 19347.2)
    .select(
        F.col("policy_id").alias("ref_id"),
        F.col("id").alias("key_id"),
        F.lit("claim").alias("src"),
    )
)

unioned = (
    adjuster_side
    .unionByName(claim_side)
    .distinct()
)

result = (
    unioned
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("src")
)
