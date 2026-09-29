from pyspark.sql import functions as F

remaining_keys = (
    df_orders
    .select("store_id")
    .intersectAll(
        df_orders
        .filter(F.col("amount") <= 1420)
        .select("store_id")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("store_id", "channel")
    .agg(
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
    )
    .orderBy(F.col("channel").desc_nulls_first())
)
