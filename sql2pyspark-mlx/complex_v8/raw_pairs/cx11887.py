from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("moved_at") < F.lit("2025-02-12 13:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

move_type_stats = (
    prepared
    .groupBy("move_type")
    .agg(F.avg("qty").alias("avg_qty"))
)

outliers = (
    prepared.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.qty") > F.col("g.avg_qty"))
    .select(
        "s.*",
        "g.avg_qty",
        F.round(F.col("s.qty") - F.col("g.avg_qty"), 2).alias("qty_minus_avg"),
    )
)

with_details_sku = (
    outliers.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.unit_price_bucket",
        "s.avg_qty",
        "s.qty_minus_avg",
        F.col("sk.category").alias("sku_category"),
        "sk.sku_code",
    )
)

enriched = (
    with_details_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "unit_price_bucket",
        "avg_qty",
        "sku_category",
        "sku_code",
        F.round(F.col("avg_qty") / F.nullif(F.col("qty_minus_avg"), F.lit(0)), 3).alias("avg_qty_per_qty_minus_avg"),
        F.concat(F.col("sku_category"), F.lit("#"), F.col("sku_id").cast("string")).alias("sku_category_ref"),
    )
)

enriched_2 = (
    enriched.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.col("w.opened_on").alias("warehouse_opened_on"))
)

result = (
    enriched_2
    .filter(F.col("warehouse_opened_on") < F.lit("2025-02-14").cast("date"))
    .select(
        "id",
        "sku_id",
        "avg_qty",
        "sku_category",
        "sku_code",
        "avg_qty_per_qty_minus_avg",
        "sku_category_ref",
        "warehouse_opened_on",
        F.length(F.trim(F.col("sku_code"))).alias("sku_code_len"),
    )
    .orderBy(
        F.col("sku_code_len").asc_nulls_last(),
        F.col("warehouse_opened_on").desc(),
    )
)
