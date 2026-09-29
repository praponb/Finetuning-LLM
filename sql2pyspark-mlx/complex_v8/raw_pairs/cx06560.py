from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        F.col("id").alias("ref_id"),
        "reported_at",
        F.col("adjuster_id").alias("qty"),
    )
)

claim_side_2 = (
    df_claims
    .filter((F.col("approved_amount") <= 32510.7) & (F.month(F.col("reported_at")).isin(2, 3)))
    .select(
        F.col("adjuster_id").alias("ref_id"),
        "reported_at",
        F.col("id").alias("qty"),
    )
)

result = claim_side.unionByName(claim_side_2)
