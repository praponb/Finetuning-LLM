from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("product_id") == 13)
    .groupBy("product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("t.unit_price_range"),
        F.col("t.unique_products"),
    )
    .orderBy(F.col("unique_products").desc())
)
