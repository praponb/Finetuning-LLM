from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("unit_price") < 120)
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("oibp.quantity_range"),
        F.col("oibp.stddev_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc(), "id")
    .limit(5)
)
