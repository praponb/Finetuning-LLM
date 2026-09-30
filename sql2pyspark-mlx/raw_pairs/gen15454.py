from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("unit_price").between(160, 380))
        .select("product_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("product_id")
    .agg(
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
    )
    .orderBy(F.col("unit_price_stddev").desc_nulls_first())
)
