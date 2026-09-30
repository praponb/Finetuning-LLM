from pyspark.sql import functions as F

claim_rows = df_claims.select(F.col("claim_type").alias("name"), F.col("claim_date").alias("day"))

holder_side = (
    df_policyholders
    .filter(F.upper(F.trim(F.col("holder_name"))).like("L%"))
    .select("segment", "joined_on")
)

result = (
    claim_rows
    .union(holder_side)
    .orderBy("day", "name")
)
