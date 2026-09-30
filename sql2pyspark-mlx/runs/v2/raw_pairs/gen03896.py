from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy("order_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
    .orderBy(F.col("order_id").desc())
    .limit(5)
)
