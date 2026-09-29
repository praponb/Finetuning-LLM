from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("account_id").isin([34, 37]) & (F.col("txn_time") < "2024-07-06"))
)

agg = (
    valid_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"), "account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    agg
    .filter(F.col("stddev_amount") >= 26)
    .orderBy(F.col("account_id").desc(), "stddev_amount")
)
