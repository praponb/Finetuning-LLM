from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 1500)
    .groupBy("is_flagged", F.year("txn_time").alias("txn_year"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .filter(F.col("stddev_amount") > 65)
)
