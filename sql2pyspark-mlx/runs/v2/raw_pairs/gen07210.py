from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("stddev_amount") > 21)
    .orderBy("order_id")
)
