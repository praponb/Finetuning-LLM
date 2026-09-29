from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 150, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(F.min("unit_price").alias("min_unit_price"))
)
