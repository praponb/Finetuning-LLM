from pyspark.sql import functions as F

src = df_transactions.filter(F.col("amount") >= 3900)

agg = (
    src
    .groupBy("txn_type", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
        F.max("txn_time").alias("latest_txn_time"),
    )
)

result = (
    agg
    .orderBy("txn_month", "latest_txn_time", "txn_type")
    .limit(20)
)
