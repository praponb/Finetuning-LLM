from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .cube(
        F.col("c.country").alias("country"),
        F.col("c.segment").alias("segment"),
        F.col("src.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.balance")).alias("max_balance"),
    )
    .orderBy("account_type")
)
