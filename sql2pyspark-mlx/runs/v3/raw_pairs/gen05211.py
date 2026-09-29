from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("oibp.unique_products"),
        F.col("oibp.stddev_quantity"),
    )
    .orderBy(F.col("cost").desc())
)
