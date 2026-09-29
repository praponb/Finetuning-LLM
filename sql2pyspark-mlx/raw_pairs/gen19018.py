from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 10) & (F.col("unit_price") > 200) & (F.col("quantity") == 7))
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 380, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
    .orderBy(F.col("avg_quantity").desc(), F.col("unit_price_bucket").desc())
)
