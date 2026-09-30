from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.supplier_id").isNotNull())
    .groupBy(
        F.when(F.col("p.price") >= 950, "high").when(F.col("p.price") >= 330, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"))
    .orderBy("price_bucket")
)
