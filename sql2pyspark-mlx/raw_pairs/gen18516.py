from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id", "status")
    .agg(
        F.count("balance").alias("balance_count"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)
