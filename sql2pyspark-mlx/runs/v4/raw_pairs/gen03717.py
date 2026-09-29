from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)
