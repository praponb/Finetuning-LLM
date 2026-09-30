from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        F.concat(F.col("operator"), F.lit("#"), F.col("id").cast("string")).alias("operator_ref"),
    )
)

with_parent = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.upper(F.trim(F.col("w.wh_code"))).like("B%"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.operator_ref",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

derived = (
    with_parent
    .filter(F.col("operator_ref").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "operator",
        "unit_price",
        "operator_ref",
        "warehouse_opened_on",
        "warehouse_wh_code",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.to_date(F.col("moved_at")).alias("moved_at_date_2"),
    )
)

warehouse_id_stats = (
    derived
    .groupBy("warehouse_id")
    .agg(F.avg("move_type_len").alias("avg_move_type_len"))
)

above_average = (
    derived.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.move_type_len") > F.col("g.avg_move_type_len"))
    .select(
        "s.*",
        "g.avg_move_type_len",
        F.round(F.col("s.move_type_len") - F.col("g.avg_move_type_len"), 2).alias("move_type_len_minus_avg"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.max("unit_price").alias("max_move_unit_price"))
)

profiled = (
    above_average.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.max_move_unit_price")
)

warehouse_id_stats_2 = (
    profiled
    .groupBy("warehouse_id")
    .agg(F.percentile_approx("max_move_unit_price", 0.5).alias("median_max_move_unit_price"))
)

outliers = (
    profiled.alias("s")
    .join(warehouse_id_stats_2.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.max_move_unit_price") >= F.col("g.median_max_move_unit_price"))
    .select(
        "s.*",
        "g.median_max_move_unit_price",
        F.round(F.col("s.max_move_unit_price") - F.col("g.median_max_move_unit_price"), 2).alias("max_move_unit_price_minus_median"),
    )
)

result = (
    outliers
    .select(
        "id",
        "warehouse_id",
        "unit_price",
        "operator_ref",
        "warehouse_wh_code",
        "move_type_len",
        "avg_move_type_len",
        "move_type_len_minus_avg",
        "max_move_unit_price",
        "median_max_move_unit_price",
        "max_move_unit_price_minus_median",
        F.abs(F.col("move_type_len_minus_avg") - -1.4).alias("move_type_len_minus_avg_distance"),
        F.coalesce(F.col("move_type_len"), F.lit(0)).alias("move_type_len_filled"),
    )
)
