from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged", F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0)).alias("amazon_count"),
    )
    .filter(F.col("amazon_count") < 77)
    .drop("amazon_count")
    .orderBy("stddev_amount")
)
