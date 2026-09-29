from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.method")))).alias("method_values"),
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
    )
    .orderBy("status")
)
