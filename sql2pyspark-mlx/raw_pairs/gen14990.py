from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 350)
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") >= 7.5)
    .select("quantity_bucket", "avg_unit_price", "max_quantity")
    .orderBy("avg_unit_price")
)
