from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([9, 16, 23, 40]) | F.col("product_id").isNull())
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        F.when(F.col("unit_price") >= 450, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .filter(F.col("total_unit_price") >= 2100)
)
