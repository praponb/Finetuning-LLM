from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("unit_price") >= 310)
    .withColumn("quantity_rounded", F.round("quantity", 0))
)

summary = (
    recent_order_items
    .groupBy("product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    summary
    .filter(F.col("unit_price_count") >= 7)
    .select("product_id", "max_unit_price", "unit_price_count")
    .orderBy("product_id")
)
