from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 310, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy(F.col("total_unit_price").desc(), "quantity_count")
)
