from pyspark.sql import functions as F

kept_keys = (
    df_order_items
    .select("id")
    .intersectAll(
        df_order_items
        .filter(F.col("product_id").isin([24, 39]))
        .select("id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .rollup("order_id")
    .agg(
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.stddev_samp("unit_price").alias("unit_price_stddev_2"),
    )
    .filter(F.col("avg_unit_price") > 0)
)
