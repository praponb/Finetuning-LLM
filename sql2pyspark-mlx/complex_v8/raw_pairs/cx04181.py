from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("move_type") != "ADJUST") & (F.col("warehouse_id") >= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
        F.abs(F.col("unit_price") - 298.9).alias("unit_price_distance"),
    )
)

enriched = (
    filtered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

sku_id_stats = (
    enriched
    .groupBy("sku_id")
    .agg(F.percentile_approx("unit_price", 0.5).alias("median_unit_price"))
)

outliers = (
    enriched.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.unit_price") >= F.col("g.median_unit_price"))
    .select(
        "s.*",
        "g.median_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.median_unit_price"), 2).alias("unit_price_minus_median"),
    )
)

per_group = (
    outliers
    .groupBy(
        F.date_trunc("month", "warehouse_opened_on").alias("warehouse_opened_on_month"),
        "sku_id",
    )
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.round(F.sum("unit_price_distance"), 2).alias("total_unit_price_distance"),
        F.max("warehouse_opened_on").alias("last_warehouse_opened_on"),
        F.count("*").alias("cnt"),
        F.sum("unit_price_distance").alias("total_unit_price_distance_2"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("move_count"),
        F.count(F.when(F.col("move_type").isin("ADJUST", "OUTBOUND", "TRANSFER"), 1)).alias("move_move_type_count"),
    )
)

with_counts = (
    per_group.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

result = (
    with_counts
    .select(
        "warehouse_opened_on_month",
        "sku_id",
        "operator_list",
        "total_unit_price_distance",
        "last_warehouse_opened_on",
        "cnt",
        "total_unit_price_distance_2",
        "last_moved_at",
        "move_count",
        "move_move_type_count",
    )
    .orderBy("total_unit_price_distance", "warehouse_opened_on_month", "sku_id")
    .limit(20)
)
