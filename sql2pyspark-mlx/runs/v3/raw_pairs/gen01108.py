from pyspark.sql import functions as F

result = (
    df_order_items
    .cube(
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.max("unit_price").alias("max_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy(F.col("max_unit_price").desc(), "unit_price_bucket")
)
