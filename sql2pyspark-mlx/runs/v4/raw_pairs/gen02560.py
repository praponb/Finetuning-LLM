from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") > 111) & F.col("quantity").between(123, 228))
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)
