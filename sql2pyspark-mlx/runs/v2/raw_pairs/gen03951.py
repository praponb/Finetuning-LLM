from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([19, 38, 40]))
    .groupBy(
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 100, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") >= 36)
    .drop("total_quantity")
)
