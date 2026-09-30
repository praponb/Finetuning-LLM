from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .filter(F.col("stddev_amount") >= 39)
    .select("txn_month", "stddev_amount", "merchant_values")
    .orderBy("stddev_amount", "txn_month")
    .limit(50)
)
