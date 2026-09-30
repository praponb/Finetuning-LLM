from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([9, 16, 23, 30]))
    .select(
        "id",
        "merchant",
        "account_id",
        F.dayofmonth("txn_time").alias("txn_time_day"),
    )
    .orderBy("txn_time_day", "id")
    .limit(1)
)
