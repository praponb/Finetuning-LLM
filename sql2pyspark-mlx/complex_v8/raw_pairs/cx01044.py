from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter((F.col("holder_id") >= 6) & (F.col("premium") > 2013.8))
    .select(
        F.col("product").alias("name"),
        F.col("end_date").alias("day"),
        F.col("status").alias("name_2"),
        F.lit("policy").alias("origin"),
    )
)

holder_side = (
    df_policyholders
    .filter((F.col("segment").isNotNull()) & (F.year(F.col("joined_on")) == 2025))
    .select(
        F.col("segment").alias("name"),
        F.col("joined_on").alias("day"),
        F.col("region").alias("name_2"),
        F.lit("holder").alias("origin"),
    )
)

result = (
    policy_side
    .unionByName(holder_side)
    .distinct()
)
