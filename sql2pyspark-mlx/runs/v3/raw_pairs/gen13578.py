from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"), "merchant")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") > 11000)
)
