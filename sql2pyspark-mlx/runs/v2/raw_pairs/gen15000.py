from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 1900)
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.count("*").alias("transaction_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)
