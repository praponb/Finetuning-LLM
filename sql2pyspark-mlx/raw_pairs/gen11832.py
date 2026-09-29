from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 76)
)
