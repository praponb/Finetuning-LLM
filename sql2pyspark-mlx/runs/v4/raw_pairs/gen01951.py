from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("quantity") <= 3)
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

summary = (
    base
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.sum("quantity_filled").alias("total_quantity_filled"),
    )
)

result = (
    summary
    .filter(F.col("total_unit_price") < 1100)
    .orderBy("order_id")
)
