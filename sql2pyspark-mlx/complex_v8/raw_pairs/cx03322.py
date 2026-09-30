from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-07-07").cast("date")))
    .select(F.col("status").alias("name"), F.col("premium").alias("amount"))
)

policy_side = (
    df_policies
    .filter((F.col("status") == "LAPSED") & (F.year(F.col("start_date")) == 2025))
    .select("status", "premium")
)

stacked = policy_rows.union(policy_side)

result = (
    stacked
    .groupBy("name")
    .agg(F.count("*").alias("n_rows"), F.sum("amount").alias("total_amount"))
    .orderBy("name")
)
