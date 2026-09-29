from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isin([12, 21, 30, 37]) & ~F.col("status").isin(["OPEN", "FROZEN"]))
    .withColumn("balance_whole", F.ceil("balance"))
)

summary = (
    base
    .groupBy("status", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.count("*").alias("account_count"),
    )
)

result = (
    summary
    .select("status", "opened_month", "latest_opened_date", "account_count")
    .orderBy("status")
)
