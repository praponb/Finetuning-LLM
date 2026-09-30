from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("order_id").isin([10, 38]))
    .groupBy("product_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.total_unit_price") > 1300)
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("t.total_unit_price"))
    .orderBy(F.col("total_unit_price").desc())
)
