from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("p.supplier_id").isNotNull())
    .groupBy(F.col("p.brand"))
    .agg(
        F.countDistinct(F.col("oi.unit_price")).alias("unique_products"),
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
    )
)
