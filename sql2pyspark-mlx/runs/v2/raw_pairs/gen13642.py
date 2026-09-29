from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .orderBy(F.col("amount_range").desc())
)
