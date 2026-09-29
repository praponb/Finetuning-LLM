from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.hour("txn_time").alias("txn_hour"),
        F.to_date("txn_time").alias("txn_day"),
        "merchant",
    )
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("unique_merchants").desc(), "txn_day")
)
