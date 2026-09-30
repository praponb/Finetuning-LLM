from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .select(
        F.col("product").alias("category"),
        "status",
        F.lit("policy").alias("src"),
    )
)

holder_rows = (
    df_policyholders
    .filter(F.col("region") != "West")
    .select(
        F.col("holder_name").alias("category"),
        F.col("region").alias("status"),
        F.lit("holder").alias("src"),
    )
)

policy_side = (
    df_policies
    .select(
        F.col("status").alias("category"),
        F.col("product").alias("status"),
        F.lit("policy_3").alias("src"),
    )
)

result = (
    policy_rows
    .unionByName(holder_rows)
    .unionByName(policy_side)
    .orderBy("category", "status")
)
