from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([8, 24, 38]) & (F.col("quantity") != 5))
    .cube(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.sum("quantity").alias("total_quantity"),
    )
)
