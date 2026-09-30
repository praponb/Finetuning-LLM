from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([18, 35]))
    .groupBy(
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
        F.sum("quantity").alias("total_quantity"),
    )
)
