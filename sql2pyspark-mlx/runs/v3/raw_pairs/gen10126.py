from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.countDistinct(F.col("oi.unit_price")).alias("unique_products"),
    )
    .orderBy(F.col("supplier_id").desc(), "unique_products")
    .limit(5)
)
