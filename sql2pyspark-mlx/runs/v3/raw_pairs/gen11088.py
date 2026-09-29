from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 390, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.max("quantity").alias("max_quantity"))
    .filter(F.col("max_quantity") >= 8.1)
)
