from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .filter(F.col("status") != "CANCELLED")
    .select(
        F.col("product").alias("name"),
        F.col("end_date").alias("day"),
        F.lit("policy").alias("src"),
    )
)

policy_rows_2 = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("premium") >= 2096.6))
    .select("product", "end_date", F.lit("policy_2").alias("src"))
)

holder_rows = df_policyholders.select("holder_name", "joined_on", F.lit("holder").alias("src"))

result = (
    policy_rows
    .union(policy_rows_2)
    .union(holder_rows)
    .orderBy(F.col("src").desc())
)
