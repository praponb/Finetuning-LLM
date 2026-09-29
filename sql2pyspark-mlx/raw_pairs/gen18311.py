from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
        F.sort_array(F.collect_set(F.col("p.method"))).alias("method_list"),
    )
)
