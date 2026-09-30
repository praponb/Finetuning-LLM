from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() | F.col("unit_price").between(200, 280))
    .groupBy(
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(F.sum("quantity").alias("total_quantity"))
)
