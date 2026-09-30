from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([5, 14, 21]))
    .groupBy("product_id", "order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("stddev_quantity") >= 65)
    .select("product_id", "order_id", "stddev_quantity", "max_quantity")
    .orderBy("max_quantity")
)
