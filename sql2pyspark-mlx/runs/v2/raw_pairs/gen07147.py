from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.count("*").alias("cnt"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
)
