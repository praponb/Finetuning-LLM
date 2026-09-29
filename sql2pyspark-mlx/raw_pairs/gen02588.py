from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isNull() & (F.col("opened_date") > "2024-06-01"))
    .withColumn("balance_whole", F.floor("balance"))
)

accounts_stats = (
    base
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
)

result = (
    accounts_stats
    .filter(F.col("avg_balance") > 56)
    .orderBy(F.col("opened_month").desc(), "avg_balance")
)
