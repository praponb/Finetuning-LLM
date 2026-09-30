from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.city").alias("city"),
        F.col("dim.country").alias("country"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.balance")).alias("min_balance"),
    )
    .orderBy("country")
)
