from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("merchant") == "Starbucks")

summary = (
    filtered_transactions
    .groupBy(
        F.date_trunc("MONTH", "txn_time").alias("txn_month"),
        F.quarter("txn_time").alias("txn_quarter"),
    )
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = summary.orderBy("stddev_amount")
