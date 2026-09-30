from pyspark.sql import functions as F

prepared = df_policies.filter(F.col("premium") <= 4417.0)

policy_rows = (
    prepared
    .filter((F.year(F.col("end_date")) == 2026) & (F.col("product").isin("AUTO", "LIFE")))
    .select("holder_id")
)

claim_side = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("adjuster_id") <= 4))
    .select(F.col("adjuster_id").alias("holder_id"))
)

result = (
    policy_rows
    .intersectAll(claim_side)
    .orderBy("holder_id")
)
