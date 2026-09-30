from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .cube(F.date_trunc("MONTH", "txn_time").alias("txn_month"), "account_id")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
    .orderBy(F.col("account_id").desc(), F.col("latest_txn_time").desc())
)
