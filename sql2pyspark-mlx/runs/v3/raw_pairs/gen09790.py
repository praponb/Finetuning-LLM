from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 15)
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") < 2.9)
    .orderBy(F.col("quantity_bucket").desc())
)
