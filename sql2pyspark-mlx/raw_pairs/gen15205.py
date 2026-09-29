from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy("stddev_quantity", "max_quantity")
)
