from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"))
    .agg(
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
        F.count("*").alias("num_order_items"),
    )
    .orderBy("stddev_unit_price", "order_month")
)
