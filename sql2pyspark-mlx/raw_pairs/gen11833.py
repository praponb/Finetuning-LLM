from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.segment").alias("segment"), F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.balance")).alias("min_balance"),
    )
)
