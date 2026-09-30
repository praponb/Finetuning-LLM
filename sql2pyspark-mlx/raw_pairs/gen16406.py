from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 380, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .orderBy(F.col("order_id").desc())
)
