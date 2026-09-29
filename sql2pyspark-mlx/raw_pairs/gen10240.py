from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("unit_price") >= 340)
    .select(
        "*",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

result = (
    filtered
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 400, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .orderBy(F.col("unit_price_tier").desc())
)
