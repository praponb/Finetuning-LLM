from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.quantity_range") > 30)
    .select(F.col("p.price"), F.col("t.quantity_range"))
    .orderBy(F.col("quantity_range").desc())
)
