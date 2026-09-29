from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 47000)
    .groupBy("account_type", "customer_id")
    .agg(
        F.count("balance").alias("balance_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
    .orderBy(F.col("customer_id").desc(), "account_type")
    .limit(10)
)
