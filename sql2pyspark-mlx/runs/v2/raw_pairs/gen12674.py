from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 180) & (F.col("quantity") < 7) & F.col("unit_price").isNull())
    .groupBy("order_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .orderBy(F.col("max_unit_price").desc(), "order_id")
)
