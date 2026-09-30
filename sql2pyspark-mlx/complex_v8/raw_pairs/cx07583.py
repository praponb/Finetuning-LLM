from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter(F.col("joined_on") < F.lit("2025-09-02").cast("date"))
    .select("holder_name", "id", F.lit("holder").alias("source"))
)

policy_side = (
    df_policies
    .filter((F.col("product") == "HEALTH") & (F.col("holder_id").isNotNull()))
    .select(
        F.col("status").alias("holder_name"),
        F.col("holder_id").alias("id"),
        F.lit("policy").alias("source"),
    )
)

result = (
    holder_side
    .union(policy_side)
    .distinct()
    .orderBy("holder_name")
)
