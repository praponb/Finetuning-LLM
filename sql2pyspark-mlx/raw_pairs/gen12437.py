from pyspark.sql import functions as F

matched_keys = (
    df_inventory
    .select("store_id", "product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("quantity").isNotNull())
        .select("store_id", "product_id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("store_id")
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .filter(F.col("quantity_stddev") >= 0)
)
