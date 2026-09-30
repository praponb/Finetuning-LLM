from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(~(F.col("quantity").between(5, 6) & F.col("product_id").isNotNull() & (F.col("quantity") != 8)))
    .withColumn("unit_price_rounded", F.round("unit_price", 2))
)

agg = (
    recent_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    agg
    .select("order_id", "stddev_quantity", "total_unit_price")
    .orderBy(F.col("stddev_quantity").desc())
)
