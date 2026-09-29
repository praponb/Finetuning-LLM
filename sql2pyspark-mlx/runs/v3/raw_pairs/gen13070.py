from pyspark.sql import functions as F

base = df_inventory.filter(F.col("quantity") == 121)

inventory_stats = (
    base
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 196, "high").when(F.col("quantity") >= 36, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    inventory_stats
    .filter(F.col("stddev_quantity") < 27)
    .orderBy("quantity_bucket")
)
