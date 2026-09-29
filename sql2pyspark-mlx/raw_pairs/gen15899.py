from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.country").alias("country"), F.col("src.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.balance")).alias("max_balance"),
    )
)
