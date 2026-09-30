from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & F.col("product_id").isNull())
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .orderBy("stddev_quantity")
)
