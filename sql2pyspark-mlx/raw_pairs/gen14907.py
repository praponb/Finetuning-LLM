from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") >= 8)
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
    .orderBy(F.col("order_id").desc(), "quantity_bucket")
    .limit(5)
)
