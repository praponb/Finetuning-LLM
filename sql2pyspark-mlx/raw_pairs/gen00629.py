from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"), "merchant")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("total_amount") >= 23000)
)
