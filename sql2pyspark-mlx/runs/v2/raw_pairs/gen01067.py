from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id").isin([3, 12, 22, 23]) & F.col("product_id").isin([11, 20, 39])) | (F.col("quantity") < 3))
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)
