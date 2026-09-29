from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("p.brand") == "Stark")
    .groupBy(
        F.when(F.col("p.stock_qty") >= 327, "high").when(F.col("p.stock_qty") >= 83, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") > 54)
    .orderBy("stock_qty_bucket", F.col("stddev_quantity").desc())
)
