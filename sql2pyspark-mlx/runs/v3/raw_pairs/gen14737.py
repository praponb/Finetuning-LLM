from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("customer_id") == 21) & F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
    .withColumn("balance_rounded", F.round("balance", 0))
)

agg = (
    filtered_accounts
    .groupBy("status")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.round(F.avg("balance_rounded"), 2).alias("avg_balance_rounded"),
    )
)

result = (
    agg
    .filter(F.col("avg_balance_rounded") < 31)
    .select("status", "min_opened_date", "avg_balance_rounded")
)
