from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter(((F.col("unit_price") < 210) & (F.col("quantity") > 8)) | F.col("unit_price").between(130, 280))
)

order_items_stats = (
    filtered_order_items
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    order_items_stats
    .filter(F.col("max_quantity") > 4.5)
    .select("product_id", "max_quantity", "stddev_quantity")
    .orderBy("max_quantity")
)
