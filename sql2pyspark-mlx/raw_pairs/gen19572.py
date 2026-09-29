from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("store_id").isin([1, 4, 5]))
        .select("product_id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("quantity") >= 187, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
        F.date_trunc("quarter", "last_restock").alias("quarter_start"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.sum("quantity").alias("total_quantity"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
)
