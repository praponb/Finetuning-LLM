from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~((F.col("unit_price") < 180) & F.col("product_id").isin([7, 32])))
    .groupBy("product_id", "order_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .filter(F.col("avg_unit_price") >= 73)
    .orderBy(F.col("avg_unit_price").desc(), "product_id", "order_id")
    .limit(3)
)
