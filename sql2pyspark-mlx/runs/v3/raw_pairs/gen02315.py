from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), "city")
    .limit(50)
)
