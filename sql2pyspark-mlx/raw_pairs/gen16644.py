from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") != "SAVINGS") & F.col("customer_id").isin([8, 21, 29, 38]))
    .groupBy("account_type", "customer_id")
    .agg(
        F.count("balance").alias("balance_count"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("total_balance") > 267000)
    .orderBy(F.col("balance_count").desc(), "customer_id")
)
