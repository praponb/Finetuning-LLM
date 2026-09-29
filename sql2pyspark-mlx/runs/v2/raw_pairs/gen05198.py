from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") < 5.5)
    .select("order_id", "stddev_unit_price", "min_quantity")
    .orderBy(F.col("min_quantity").desc(), "order_id")
    .limit(50)
)
