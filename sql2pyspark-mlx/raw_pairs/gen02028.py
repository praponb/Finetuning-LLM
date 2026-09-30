from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        "product_id",
        F.when(F.col("unit_price") >= 350, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("unit_price_tier").asc_nulls_first())
)
