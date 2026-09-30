from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("*").alias("order_item_count"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .orderBy(F.col("unit_price_count").desc(), F.col("unit_price_range").desc())
)
