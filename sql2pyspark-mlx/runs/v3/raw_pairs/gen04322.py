from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.stddev_quantity") > 50)
    .select(
        F.col("products.brand"),
        F.col("products.price"),
        F.col("t.stddev_quantity"),
    )
)
