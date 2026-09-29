from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([18, 20]))
    .groupBy("product_id", "order_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") >= 56)
)
