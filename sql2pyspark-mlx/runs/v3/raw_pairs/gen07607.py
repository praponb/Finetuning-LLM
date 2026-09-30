from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") >= 11000)
    .drop("total_amount")
)
