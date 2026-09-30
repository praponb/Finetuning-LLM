from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 8]))
    .groupBy(
        F.when(F.col("quantity") >= 256, "high").when(F.col("quantity") >= 73, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
    .filter(F.col("stddev_quantity") > 58)
    .select("quantity_bucket", "stddev_quantity", "last_last_restock")
    .orderBy("quantity_bucket")
)
