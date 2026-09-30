from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") == "Tokyo")
    .groupBy(
        F.when(F.col("p.price") >= 1160, "high").when(F.col("p.price") >= 370, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") >= 33)
)
