from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.sum("balance").alias("total_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .filter(F.col("balance_range") < 60)
    .select("status", "total_balance", "balance_range")
)
