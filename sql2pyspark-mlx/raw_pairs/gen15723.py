from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 410, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
        F.when(F.col("quantity") >= 8, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy("distinct_product_ids")
)
