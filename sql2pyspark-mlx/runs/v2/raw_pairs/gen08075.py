from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(
        F.when(F.col("s.rating") >= 4.1, "high").when(F.col("s.rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
    )
    .orderBy("rating_bucket")
)
