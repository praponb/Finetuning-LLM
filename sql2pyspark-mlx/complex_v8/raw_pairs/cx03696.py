from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
    )
)

joined_warehouse = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

warehouse_city_stats = (
    joined_warehouse
    .groupBy("warehouse_city")
    .agg(F.max("warehouse_capacity").alias("max_warehouse_capacity"))
)

outliers = (
    joined_warehouse.alias("s")
    .join(warehouse_city_stats.alias("g"), F.col("g.warehouse_city") == F.col("s.warehouse_city"), "inner")
    .filter(F.col("s.warehouse_capacity") == F.col("g.max_warehouse_capacity"))
    .select(
        "s.*",
        "g.max_warehouse_capacity",
        F.round(F.col("s.warehouse_capacity") - F.col("g.max_warehouse_capacity"), 2).alias("warehouse_capacity_minus_max"),
    )
)

summary = (
    outliers
    .filter(F.col("warehouse_capacity") >= 2614)
    .groupBy(
        F.date_trunc("month", "warehouse_opened_on").alias("warehouse_opened_on_month"),
    )
    .agg(
        F.percentile_approx("max_warehouse_capacity", 0.5).alias("median_max_warehouse_capacity"),
        F.min("max_warehouse_capacity").alias("min_max_warehouse_capacity"),
        F.max("warehouse_opened_on").alias("last_warehouse_opened_on"),
        F.max("unit_price_per_qty").alias("max_unit_price_per_qty_check"),
    )
    .filter(F.col("max_unit_price_per_qty_check") > 3.0)
    .drop("max_unit_price_per_qty_check")
)

result = (
    summary
    .select(
        "warehouse_opened_on_month",
        "median_max_warehouse_capacity",
        "min_max_warehouse_capacity",
        "last_warehouse_opened_on",
    )
)
