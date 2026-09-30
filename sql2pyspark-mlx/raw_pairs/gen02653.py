from pyspark.sql import functions as F

kept_keys = (
    df_order_items
    .select("product_id", "id")
    .exceptAll(
        df_order_items
        .filter(F.col("quantity") != 3)
        .select("product_id", "id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("order_id")
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.count("*").alias("row_count"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("distinct_order_ids") > 1)
)
