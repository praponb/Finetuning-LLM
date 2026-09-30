from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isNotNull())
    .groupBy(F.col("p.brand"))
    .agg(
        F.sum((F.col("oi.quantity") * F.col("oi.unit_price"))).alias("revenue"),
        F.max(F.col("oi.quantity")).alias("max_quantity"),
    )
)
