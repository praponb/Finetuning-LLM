from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 11)
    .groupBy(
        F.when(F.col("balance") >= 88000, "high").when(F.col("balance") >= 7000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
    )
    .orderBy(F.col("balance_bucket").desc())
)
