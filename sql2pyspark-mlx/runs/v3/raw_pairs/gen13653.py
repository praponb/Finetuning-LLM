from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 6]) & F.col("store_id").isin([2, 6]) & (F.col("quantity") > 192))
    .groupBy(
        F.when(F.col("quantity") >= 241, "high").when(F.col("quantity") >= 97, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") < 27)
    .orderBy("quantity_bucket")
)
