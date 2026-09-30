from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .filter(F.col("unique_txn_types") > 5)
    .select("txn_type", "txn_day", "unique_txn_types", "first_txn_time")
)
