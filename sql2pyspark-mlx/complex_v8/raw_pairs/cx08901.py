from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("start_date") >= F.lit("2024-08-27").cast("date")))
    .select("product", "start_date", F.lit("policy").alias("record_type"))
)

holder_rows = (
    df_policyholders
    .filter(F.col("segment").isin("RETAIL", "VIP"))
    .select("region", "joined_on", F.lit("holder").alias("record_type"))
)

claim_side = df_claims.select("claim_type", "claim_date", F.lit("claim").alias("record_type"))

result = (
    policy_side
    .union(holder_rows)
    .union(claim_side)
)
