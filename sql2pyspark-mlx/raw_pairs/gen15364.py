from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2023-06-01")
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 23)
    .orderBy(F.col("avg_amount").desc())
)
