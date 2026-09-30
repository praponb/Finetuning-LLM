from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 6, 7]))
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") > 4)
    .drop("unique_stores")
    .orderBy(F.col("stddev_quantity").desc())
)
