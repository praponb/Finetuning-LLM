from pyspark.sql import functions as F

adjuster_side = df_adjusters.select("level", "adjuster_name")

claim_rows = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp"))
    .select(
        F.col("policy_id").alias("level"),
        F.col("claim_type").alias("adjuster_name"),
    )
)

result = adjuster_side.intersectAll(claim_rows)
