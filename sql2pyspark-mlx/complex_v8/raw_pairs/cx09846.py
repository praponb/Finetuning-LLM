from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .filter(F.col("segment").isin("VIP", "RETAIL"))
    .select(
        F.col("region").alias("category"),
        "joined_on",
        F.lit("holder").alias("origin"),
    )
)

policy_side = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-02-26").cast("date"))
    .select(
        F.col("status").alias("category"),
        F.col("end_date").alias("joined_on"),
        F.lit("policy").alias("origin"),
    )
)

policy_side_2 = (
    df_policies
    .select(
        F.col("status").alias("category"),
        F.col("start_date").alias("joined_on"),
        F.lit("policy").alias("origin"),
    )
)

unioned = (
    holder_rows
    .unionByName(policy_side)
    .unionByName(policy_side_2)
)

result = (
    unioned
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("joined_on").alias("latest_joined_on"),
    )
    .orderBy("origin")
)
