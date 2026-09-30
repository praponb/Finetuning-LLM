from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 5, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.max("quantity").alias("max_quantity"),
        F.max("unit_price").alias("max_unit_price"),
    )
)
