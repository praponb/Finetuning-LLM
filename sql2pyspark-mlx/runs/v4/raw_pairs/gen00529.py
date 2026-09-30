from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("stddev_quantity") >= 27)
    .select("product_id", "stddev_quantity", "row_count")
    .orderBy("stddev_quantity")
)
