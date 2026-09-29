from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 6))
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("order_item_count"))
    .filter(F.col("order_item_count") > 5)
    .orderBy(F.col("order_item_count").desc())
)
