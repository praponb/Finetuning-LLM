from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("product_id").isNull() & (F.col("quantity") > 7))
    .withColumn("unit_price_rounded", F.round("unit_price", 0))
)

summary = (
    src
    .groupBy("product_id")
    .agg(
        F.sum("unit_price_rounded").alias("total_unit_price_rounded"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = summary.select("product_id", "total_unit_price_rounded")
