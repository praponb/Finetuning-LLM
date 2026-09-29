from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.supplier_name").isNotNull())
    .groupBy(F.col("p.category"))
    .agg(
        F.max(F.col("oi.quantity")).alias("max_quantity"),
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") > 70)
)
