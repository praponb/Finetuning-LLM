from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 84000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CHECKING", 1)), F.lit(0)), 2).alias("balance_per_checking"),
        F.count("*").alias("num_rows"),
        F.avg("balance").alias("avg_balance"),
    )
)
