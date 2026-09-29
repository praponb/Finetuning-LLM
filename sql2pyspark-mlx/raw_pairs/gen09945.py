from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)

result = (
    base
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 9, "PREMIUM").otherwise("STANDARD"))
    .rollup("quantity_tier", "order_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.round(F.avg("quantity_filled"), 2).alias("avg_quantity_filled"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("avg_quantity_filled") > 0)
    .orderBy(F.col("avg_quantity_filled").desc())
)
