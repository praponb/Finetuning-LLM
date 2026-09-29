from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city").alias("city"), F.col("c.segment").alias("segment"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.balance")).alias("min_balance"),
    )
)
