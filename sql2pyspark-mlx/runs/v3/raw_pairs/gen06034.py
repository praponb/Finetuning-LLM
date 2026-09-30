from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.price"), F.col("t.quantity_count"), F.col("t.unique_orders"))
)
