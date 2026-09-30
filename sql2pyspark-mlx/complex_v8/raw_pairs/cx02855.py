from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
        F.when(F.col("qty") >= 314, "A").when(F.col("qty") >= 145, "B").otherwise("C").alias("qty_tier"),
    )
)

move_type_stats = (
    base
    .groupBy("move_type")
    .agg(F.percentile_approx("unit_price", 0.5).alias("median_unit_price"))
)

compared = (
    base.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price") >= F.col("g.median_unit_price"))
    .select(
        "s.*",
        "g.median_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.median_unit_price"), 2).alias("unit_price_minus_median"),
    )
)

joined_warehouse = (
    compared.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select("s.*", F.col("w.opened_on").alias("warehouse_opened_on"))
)

scored = (
    joined_warehouse
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator",
        "unit_price",
        "qty_tier",
        "median_unit_price",
        "unit_price_minus_median",
        F.round(F.col("unit_price_minus_median") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("unit_price_minus_median_per_unit_price"),
    )
)

combined_details = (
    scored.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.operator",
        "s.unit_price",
        "s.qty_tier",
        "s.median_unit_price",
        "s.unit_price_minus_median_per_unit_price",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

with_parent = (
    combined_details.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.operator",
        "s.unit_price",
        "s.qty_tier",
        "s.median_unit_price",
        "s.unit_price_minus_median_per_unit_price",
        "s.warehouse_capacity",
        "s.warehouse_city",
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

result = (
    with_parent
    .select("id", "median_unit_price", "unit_price_minus_median_per_unit_price")
    .unpivot(
        ids=["id"],
        values=["median_unit_price", "unit_price_minus_median_per_unit_price"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric")
)
