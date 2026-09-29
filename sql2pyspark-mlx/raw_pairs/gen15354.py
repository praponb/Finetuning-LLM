from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2024-08-01")
    .groupBy("merchant")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("txn_time").alias("latest_txn_time"),
    )
)
