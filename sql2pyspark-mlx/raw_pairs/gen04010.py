from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy("min_unit_price", "quantity_bucket", "product_id")
    .limit(1)
)
