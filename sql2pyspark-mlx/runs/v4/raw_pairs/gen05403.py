from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "status")
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.min("balance").alias("min_balance"),
    )
    .orderBy(F.col("closed_count").desc())
)
