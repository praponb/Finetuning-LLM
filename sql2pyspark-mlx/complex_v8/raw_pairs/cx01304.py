from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("move_type") != "TRANSFER") & (F.col("operator").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        F.col("w.capacity").alias("warehouse_capacity"),
        "sk.sku_code",
    )
)

sku_id_stats = (
    with_parent
    .groupBy("sku_id")
    .agg(F.percentile_approx("qty", 0.5).alias("median_qty"))
)

compared = (
    with_parent.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.qty") >= F.col("g.median_qty"))
    .select(
        "s.*",
        "g.median_qty",
        F.round(F.col("s.qty") - F.col("g.median_qty"), 2).alias("qty_minus_median"),
    )
)

result = (
    compared
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "sku_code",
        "median_qty",
        "qty_minus_median",
        F.length(F.trim(F.col("sku_code"))).alias("sku_code_len"),
    )
    .orderBy("qty")
)
