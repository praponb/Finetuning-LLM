from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id").isin([9, 10, 13, 30]) & F.col("order_id").isin([13, 30, 39])) | (F.col("product_id") == 38))
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)
