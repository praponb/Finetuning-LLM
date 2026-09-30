from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.supplier_name").isNotNull())
    .groupBy(
        F.when(F.col("s.rating") >= 4.0, "high").when(F.col("s.rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy("avg_quantity")
)
