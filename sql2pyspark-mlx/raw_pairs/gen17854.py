from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.max("balance").alias("max_balance"),
    )
    .filter(F.col("max_balance") > 37000)
    .orderBy(F.col("credit_count").desc())
)
