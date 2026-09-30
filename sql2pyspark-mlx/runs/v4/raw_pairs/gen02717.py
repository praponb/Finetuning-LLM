from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.percentile_approx("unit_price", 0.95).alias("unit_price_p95"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.stddev_samp("unit_price").alias("unit_price_stddev_2"),
    )
    .orderBy("unit_price_stddev")
)
