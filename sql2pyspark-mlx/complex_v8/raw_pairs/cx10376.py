from pyspark.sql import functions as F

adjuster_side = df_adjusters.select(F.col("adjuster_name").alias("name"))

claim_side = (
    df_claims
    .filter(F.col("status") != "PAID")
    .select("status")
)

result = (
    adjuster_side
    .subtract(claim_side)
    .orderBy("name")
)
