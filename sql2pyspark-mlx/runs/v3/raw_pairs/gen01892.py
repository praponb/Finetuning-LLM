from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.discount") < 0.14)
    .groupBy(F.quarter(F.col("o.order_date")).alias("order_quarter"))
    .agg(F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"))
)
