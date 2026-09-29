from pyspark.sql import functions as F

policy_side = df_policies.select(F.col("status").alias("label"), "product")

claim_rows = df_claims.select(F.col("status").alias("label"), F.col("claim_type").alias("product"))

policy_side_2 = (
    df_policies
    .filter(F.col("product") != "TRAVEL")
    .select(F.col("product").alias("label"), F.col("status").alias("product"))
)

result = (
    policy_side
    .unionByName(claim_rows)
    .unionByName(policy_side_2)
    .orderBy("label", "product")
)
