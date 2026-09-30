from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .orderBy(F.col("total_unit_price").desc(), "quantity_bucket")
    .limit(5)
)
