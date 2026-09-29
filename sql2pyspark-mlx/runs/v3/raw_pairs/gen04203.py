from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([26, 40]))
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.count("amount").alias("amount_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .filter(F.col("amount_count") > 5)
)
