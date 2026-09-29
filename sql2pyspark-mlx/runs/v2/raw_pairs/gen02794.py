from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 4)
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 5, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("unique_products") < 3)
    .orderBy("quantity_bucket", "total_unit_price")
    .limit(50)
)
