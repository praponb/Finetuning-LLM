from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") < 3.4)
)
