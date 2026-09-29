from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("order_id") == 17)
    .groupBy("product_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .filter(F.col("P.revenue") > 54)
    .select(F.col("p.id"), F.col("p.stock_qty"), F.col("P.revenue"))
    .orderBy("id")
    .limit(20)
)
