from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter(F.col("sku_id") <= 13)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
    )
)

operator_stats = (
    recent
    .groupBy("operator")
    .agg(F.percentile_approx("unit_price", 0.5).alias("median_unit_price"))
)

outliers = (
    recent.alias("s")
    .join(operator_stats.alias("g"), F.col("g.operator") == F.col("s.operator"), "inner")
    .filter(F.col("s.unit_price") >= F.col("g.median_unit_price"))
    .select(
        "s.*",
        "g.median_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.median_unit_price"), 2).alias("unit_price_minus_median"),
    )
)

with_parent_warehouse = (
    outliers.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.city") == "Kuala Lumpur"), "left")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    with_parent_warehouse
    .groupBy("moved_at_year", "move_type")
    .pivot("warehouse_city", ["Bangkok", "Singapore"])
    .agg(F.sum("warehouse_capacity"))
)
