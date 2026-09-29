from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & (F.col("order_id").isin([3, 15, 30]) | F.col("unit_price").isNull()))
    .groupBy(
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy("unit_price_bucket")
)
