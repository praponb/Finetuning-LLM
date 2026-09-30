from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 16)
    .groupBy("product_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
    .filter(F.col("unit_price_range") > 23)
    .select("product_id", "avg_unit_price", "unit_price_range")
    .orderBy("product_id", F.col("avg_unit_price").desc())
)
