from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .groupBy("account_type")
    .agg(
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .filter(F.col("balance_range") < 73)
    .orderBy(F.col("frozen_count").desc(), F.col("balance_range").desc())
)
