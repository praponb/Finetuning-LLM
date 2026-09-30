from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-09-27").cast("date"), F.lit("2027-03-20").cast("date")))
    .select(
        "id",
        F.col("product").alias("label"),
        "holder_id",
        F.lit("policy").alias("origin"),
    )
)

policy_side_2 = (
    df_policies
    .filter((F.col("status").isin("ACTIVE", "EXPIRED", "CANCELLED")) & (F.col("product") == "LIFE"))
    .select(
        "id",
        F.col("product").alias("label"),
        "holder_id",
        F.lit("policy_2").alias("origin"),
    )
)

policy_side_3 = (
    df_policies
    .select(
        F.col("holder_id").alias("id"),
        F.col("product").alias("label"),
        F.col("id").alias("holder_id"),
        F.lit("policy_3").alias("origin"),
    )
)

result = (
    policy_side
    .unionByName(policy_side_2)
    .unionByName(policy_side_3)
)
