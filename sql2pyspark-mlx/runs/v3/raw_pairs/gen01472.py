from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.col("customer_id") == 17) & ((F.col("balance") < 66000) | F.month("opened_date").isin([1, 5, 6])))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
)

accounts_stats = (
    recent_accounts
    .groupBy("status")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.avg("balance").alias("avg_balance"),
    )
)

result = accounts_stats
