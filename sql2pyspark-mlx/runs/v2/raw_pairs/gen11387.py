from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 8))
    .groupBy(
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 120, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .filter(F.col("total_unit_price") > 600)
    .orderBy(F.col("total_unit_price").desc(), "unit_price_bucket")
    .limit(5)
)
