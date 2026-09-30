from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["REFUND", "CREDIT", "DEBIT"]) & (F.col("account_id") == 19) & (F.col("amount") > 3300))
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)
