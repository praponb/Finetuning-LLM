from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("id", "order_id")
    .exceptAll(
        df_order_items
        .filter(F.col("order_id") == 35)
        .select("id", "order_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.count("*").alias("event_count"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
)
