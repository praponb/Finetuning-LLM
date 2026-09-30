from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 350) & (F.col("product_id") == 14))
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("unit_price").alias("max_unit_price"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") >= 350)
    .drop("avg_unit_price")
)
