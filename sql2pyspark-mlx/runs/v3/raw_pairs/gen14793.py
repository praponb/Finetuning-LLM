from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .filter(F.col("products.price") > 800)
    .groupBy(F.col("products.category"))
    .agg(
        F.avg(F.col("order_items.quantity")).alias("avg_quantity"),
        F.round(F.stddev(F.col("order_items.quantity")), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc(), "category")
)
