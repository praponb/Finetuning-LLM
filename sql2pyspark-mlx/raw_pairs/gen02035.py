from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.city").alias("city"),
        F.col("s.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.balance")).alias("max_balance"),
    )
)
