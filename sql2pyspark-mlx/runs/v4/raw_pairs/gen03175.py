from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 6)
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
    .orderBy(F.col("total_count").desc(), "quantity_bucket")
    .limit(5)
)
