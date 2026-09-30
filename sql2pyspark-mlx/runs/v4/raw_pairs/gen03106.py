from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 22) & (F.col("unit_price") >= 230))
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("unit_price").alias("total_unit_price"))
    .filter(F.col("total_unit_price") >= 1500)
)
