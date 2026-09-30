from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") > 221)
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("product_id").desc())
)
