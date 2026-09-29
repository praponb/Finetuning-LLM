from pyspark.sql import functions as F

result = (
    df_order_items
    .cube("order_id", "product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
    .orderBy(F.col("unit_price_count").desc())
)
