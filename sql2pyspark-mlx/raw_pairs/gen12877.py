from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 5))
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(F.max("quantity").alias("max_quantity"))
    .filter(F.col("max_quantity") >= 5.4)
)
