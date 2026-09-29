from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .select(
        F.lit("holder").alias("origin"),
        "segment",
        F.col("holder_name").alias("name"),
        "joined_on",
    )
)

policy_side = (
    df_policies
    .select(
        F.lit("policy").alias("origin"),
        F.col("status").alias("segment"),
        F.col("product").alias("name"),
        F.col("end_date").alias("joined_on"),
    )
)

result = (
    holder_rows
    .unionByName(policy_side)
    .distinct()
    .orderBy(F.col("origin").desc(), "name")
)
