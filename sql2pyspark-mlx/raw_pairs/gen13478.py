from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("account_id") == 23)
    .withColumn("merchant_length", F.length("merchant"))
)

txn_month_summary = (
    valid_transactions
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
)

result = (
    txn_month_summary
    .select("txn_month", "first_txn_time", "unique_txn_types")
    .orderBy("first_txn_time")
)
