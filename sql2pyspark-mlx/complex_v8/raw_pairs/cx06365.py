from pyspark.sql import functions as F

adjuster_rows = (
    df_adjusters
    .filter(F.col("level").between(1, 3))
    .select("level", F.col("id").alias("ref_id"))
)

claim_rows = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-02-18 13:00:00").cast("timestamp"))
    .select(F.col("adjuster_id").alias("level"), F.col("id").alias("ref_id"))
)

result = (
    adjuster_rows
    .unionByName(claim_rows)
    .orderBy("ref_id", "level")
)
