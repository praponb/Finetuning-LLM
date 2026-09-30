from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.rating") >= 4.2, "high").when(F.col("s.rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
)
