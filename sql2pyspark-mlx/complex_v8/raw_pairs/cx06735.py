from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
    )
)

joined_warehouse = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_adj",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

sku_id_stats = (
    joined_warehouse
    .groupBy("sku_id")
    .agg(F.percentile_approx("qty", 0.5).alias("median_qty"))
)

outliers = (
    joined_warehouse.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.qty") >= F.col("g.median_qty"))
    .select(
        "s.*",
        "g.median_qty",
        F.round(F.col("s.qty") - F.col("g.median_qty"), 2).alias("qty_minus_median"),
    )
)

result = (
    outliers
    .groupBy("moved_at", "sku_id")
    .pivot("operator", ["op_bee", "op_ann", "op_dew", "op_chan"])
    .agg(F.count("unit_price_adj"))
)
