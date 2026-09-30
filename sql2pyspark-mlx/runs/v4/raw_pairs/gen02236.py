from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 310, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
        "product_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
    )
    .filter(F.col("quantity_bit_or") > 1)
    .orderBy("product_id")
)
