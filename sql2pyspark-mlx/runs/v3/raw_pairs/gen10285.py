from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 7) & (F.col("unit_price") > 110))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 3)
)
