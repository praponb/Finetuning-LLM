from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") == 2024)
    .groupBy("status")
    .agg(
        F.max("balance").alias("max_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .filter(F.col("max_balance") > 67000)
    .select("status", "max_balance", "balance_range")
)
