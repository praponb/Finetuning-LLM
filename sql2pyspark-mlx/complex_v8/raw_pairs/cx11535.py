from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter((F.col("region").isin("East", "North", "South")) & (F.col("segment").isin("RETAIL", "VIP")))
    .select("id")
)

claim_side = df_claims.select("adjuster_id")

result = (
    holder_side
    .subtract(claim_side)
    .orderBy("id")
)
