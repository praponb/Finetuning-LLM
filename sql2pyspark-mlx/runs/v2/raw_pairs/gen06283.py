from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("balance") <= 18000) | (F.col("balance") >= 66000)))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
    )
    .filter(F.col("balance_range") > 44)
    .orderBy("opened_quarter")
)
