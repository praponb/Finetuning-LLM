from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .filter(F.col("quantity_count") < 5)
    .orderBy("quantity_bucket")
)
