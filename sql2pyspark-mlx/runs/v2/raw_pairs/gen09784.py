from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0)).alias("loan_count"),
        F.avg("balance").alias("avg_balance"),
    )
    .filter(F.col("avg_balance") > 61000)
    .drop("avg_balance")
    .orderBy(F.col("status").desc(), "loan_count")
)
