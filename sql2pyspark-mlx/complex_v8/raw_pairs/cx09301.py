from pyspark.sql import functions as F

adjuster_side = df_adjusters.select("level")

claim_side = df_claims.select(F.col("adjuster_id").alias("level"))

result = (
    adjuster_side
    .intersect(claim_side)
    .orderBy(F.col("level").desc())
)
