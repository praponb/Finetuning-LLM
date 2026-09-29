from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(((F.col("txn_time") >= "2024-03-21") & F.col("merchant").isin(["Amazon", "7-Eleven"])) | (F.col("txn_type") == "DEBIT"))
)

transactions_stats = (
    filtered_transactions
    .groupBy("merchant")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = transactions_stats.orderBy(F.col("stddev_amount").desc(), "merchant")
