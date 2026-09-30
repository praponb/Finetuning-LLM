from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("O"), F.col("p.id") == F.col("O.product_id"), "inner")
    .filter(F.col("O.revenue") > 66)
    .select(F.col("p.product_name"), F.col("p.stock_qty"), F.col("O.revenue"))
    .orderBy("revenue")
)
