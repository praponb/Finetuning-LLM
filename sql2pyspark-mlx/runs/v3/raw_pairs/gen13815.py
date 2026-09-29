from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter((F.col("unit_price") > 230) & F.col("order_id").isin([9, 23, 38]))
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
)

greatest_quantity_unit_price_bucket_summary = (
    filtered_order_items
    .groupBy(
        F.when(F.col("greatest_quantity_unit_price") >= 66, "high").when(F.col("greatest_quantity_unit_price") >= 36, "mid").otherwise("low").alias("greatest_quantity_unit_price_bucket"),
    )
    .agg(
        F.avg("greatest_quantity_unit_price").alias("avg_greatest_quantity_unit_price"),
    )
)

result = (
    greatest_quantity_unit_price_bucket_summary
    .filter(F.col("avg_greatest_quantity_unit_price") < 40)
    .orderBy(F.col("greatest_quantity_unit_price_bucket").desc())
)
