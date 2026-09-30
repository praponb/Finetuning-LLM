from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("customer_id").isNull() & F.col("customer_id").isin([8, 15, 20, 38]) & F.month("opened_date").isin([3, 7, 8]))
    .withColumn("clean_account_type", F.trim("account_type"))
)

agg = (
    recent_accounts
    .groupBy("status")
    .agg(
        F.min("balance").alias("min_balance"),
        F.count("*").alias("total_count"),
    )
)

result = (
    agg
    .select("status", "min_balance", "total_count")
    .orderBy(F.col("min_balance").desc(), "status")
    .limit(10)
)
