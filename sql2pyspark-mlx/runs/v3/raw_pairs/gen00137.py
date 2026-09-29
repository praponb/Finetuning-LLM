from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity").between(3, 5) & F.col("product_id").isNotNull()) | F.col("product_id").isNull())
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("quantity_bucket").desc(), F.col("max_quantity").desc())
    .limit(50)
)
