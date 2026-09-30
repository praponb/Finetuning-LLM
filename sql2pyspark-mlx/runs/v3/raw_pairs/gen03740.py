from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 4)
    .groupBy("order_id", "product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)
