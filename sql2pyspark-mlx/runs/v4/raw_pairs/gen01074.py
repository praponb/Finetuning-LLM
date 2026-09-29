from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("product_id").isin([7, 8, 30, 31]) | (F.col("quantity") <= 5)))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(
        F.count("quantity_filled").alias("quantity_filled_count"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    product_id_summary
    .select("product_id", "stddev_quantity")
    .orderBy(F.col("stddev_quantity").desc())
)
