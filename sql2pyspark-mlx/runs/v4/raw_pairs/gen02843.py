from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.count(F.col("oi.quantity")).alias("quantity_count"),
        F.countDistinct(F.col("oi.unit_price")).alias("unique_orders"),
    )
    .filter(F.col("quantity_count") > 2)
    .orderBy(F.col("quantity_count").desc(), F.col("supplier_id").desc())
)
