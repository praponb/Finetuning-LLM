from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("premium") >= 2111.3) & (F.col("status").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
    )
)

policy_rows = (
    prepared
    .filter(F.col("status") == "LAPSED")
    .select(
        "holder_id",
        F.col("status").alias("name"),
        "start_date",
        F.lit("policy").alias("origin"),
    )
)

policy_side = (
    df_policies
    .filter((F.col("status").isin("CANCELLED", "LAPSED")) & (F.col("premium") <= 3719.7))
    .select(
        F.col("id").alias("holder_id"),
        F.col("status").alias("name"),
        "start_date",
        F.lit("policy_2").alias("origin"),
    )
)

result = (
    policy_rows
    .union(policy_side)
    .orderBy("holder_id")
)
