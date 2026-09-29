from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") >= 2700)
    .select("txn_month", "min_amount")
    .orderBy(F.col("txn_month").desc())
)
