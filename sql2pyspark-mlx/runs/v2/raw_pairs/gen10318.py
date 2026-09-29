from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
    )
    .orderBy(F.col("brand").desc())
)
