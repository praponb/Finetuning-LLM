from pyspark.sql import functions as F

claim_side = df_claims.select("policy_id")

adjuster_rows = (
    df_adjusters
    .filter((F.col("level") >= 3) & (F.col("adjuster_name").isNotNull()))
    .select("level")
)

result = (
    claim_side
    .subtract(adjuster_rows)
    .orderBy(F.col("policy_id").desc())
)
