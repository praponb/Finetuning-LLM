from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_qty_distance = Window.partitionBy("sku_id").orderBy(F.col("qty_distance").desc(), "id")
w_moved_at_year_warehouse_id = Window.partitionBy(F.year("moved_at"), "warehouse_id")

valid_moves = (
    df_stock_moves
    .filter(F.col("move_type") == "ADJUST")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("INBOUND", "OUTBOUND"), "group_a").when(F.col("move_type") == "ADJUST", "group_b").otherwise("other").alias("move_type_group"),
        F.abs(F.col("unit_price") - 292.3).alias("unit_price_distance"),
    )
)

scored = (
    valid_moves
    .select(
        "*",
        F.concat(F.col("move_type_group"), F.lit("#"), F.col("qty").cast("string")).alias("move_type_group_ref"),
        F.abs(F.col("qty") - 332).alias("qty_distance"),
    )
)

combined_details_warehouse = (
    scored.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.city").isin("Kuala Lumpur", "Bangkok")), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_group",
        "s.unit_price_distance",
        "s.qty_distance",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

with_windows = (
    combined_details_warehouse
    .withColumn("qty_distance_change", F.col("qty_distance") - F.lag("qty_distance").over(w_sku_id_by_qty_distance))
    .withColumn("distinct_operator_in_moved_at_year_warehouse_id", F.size(F.collect_set("operator").over(w_moved_at_year_warehouse_id)))
)

combined_details_warehouse_2 = (
    with_windows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.move_type_group",
        "s.unit_price_distance",
        "s.qty_distance",
        "s.warehouse_capacity",
        "s.qty_distance_change",
        "s.distinct_operator_in_moved_at_year_warehouse_id",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity_2"),
    )
)

warehouse_city_stats = (
    combined_details_warehouse_2
    .groupBy("warehouse_city")
    .agg(F.max("warehouse_capacity").alias("max_warehouse_capacity"))
)

outliers = (
    combined_details_warehouse_2.alias("s")
    .join(warehouse_city_stats.alias("g"), F.col("g.warehouse_city") == F.col("s.warehouse_city"), "inner")
    .filter(F.col("s.warehouse_capacity") == F.col("g.max_warehouse_capacity"))
    .select(
        "s.*",
        "g.max_warehouse_capacity",
        F.round(F.col("s.warehouse_capacity") - F.col("g.max_warehouse_capacity"), 2).alias("warehouse_capacity_minus_max"),
    )
)

grouped = (
    outliers
    .filter(F.col("max_warehouse_capacity") <= 15250.0)
    .groupBy("warehouse_city", F.to_date("moved_at").alias("moved_at_day"))
    .agg(
        F.round(100 * F.count(F.when(F.col("warehouse_city") == "Singapore", 1)) / F.count("*"), 1).alias("pct_warehouse_city_singapore"),
        F.count("*").alias("cnt"),
    )
)

result = grouped.select("warehouse_city", "pct_warehouse_city_singapore", "cnt")
