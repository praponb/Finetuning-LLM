from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 222)
    .groupBy(
        F.when(F.col("quantity") >= 188, "high").when(F.col("quantity") >= 97, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") < 57)
    .orderBy("min_last_restock")
)
