from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "OPEN") & (F.year("opened_date") >= 2025))
    .groupBy(
        F.when(F.col("balance") >= 88000, "high").when(F.col("balance") >= 13000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.min("balance").alias("min_balance"),
    )
)
