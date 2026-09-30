from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("unit_price", 0.95).alias("unit_price_p95"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .filter(F.col("quantity_stddev") >= 0)
    .orderBy(F.col("quantity_tier").desc())
)
