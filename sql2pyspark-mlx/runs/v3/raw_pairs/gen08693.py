from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.order_id").isin([4, 21]))
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.countDistinct(F.col("oi.quantity")).alias("unique_products"),
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
    )
)
