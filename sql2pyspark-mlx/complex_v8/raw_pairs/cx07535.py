from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.abs(F.col("unit_price") - 289.7).alias("unit_price_distance"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type").isin("TRANSFER", "INBOUND", "OUTBOUND"), 1)).alias("move_move_type_count"),
    )
)

with_stats = (
    recent_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

combined_details = (
    with_stats.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price_distance",
        "s.move_move_type_count",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

tagged = (
    combined_details
    .select(
        "id",
        "move_type",
        "operator",
        "unit_price_distance",
        "move_move_type_count",
        "warehouse_capacity",
        "warehouse_opened_on",
        "warehouse_city",
        "sku_unit_cost",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.concat_ws("-", F.col("operator"), F.col("warehouse_city")).alias("operator_warehouse_city_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("warehouse_opened_on")).alias("days_since_warehouse_opened_on"),
    )
)

labeled = (
    tagged
    .withColumn("warehouse_opened_on_plus_30d", F.date_add(F.col("warehouse_opened_on"), 30))
)

screened = (
    labeled
    .crossJoin(labeled.agg(F.avg("sku_unit_cost").alias("overall_avg_sku_unit_cost")))
    .filter(F.col("sku_unit_cost") > F.col("overall_avg_sku_unit_cost"))
    .drop("overall_avg_sku_unit_cost")
)

result = (
    screened
    .select(
        "id",
        "operator",
        "unit_price_distance",
        "move_move_type_count",
        "warehouse_opened_on",
        "sku_unit_cost",
        "operator_warehouse_city_label",
        "days_since_warehouse_opened_on",
        "warehouse_opened_on_plus_30d",
    )
    .orderBy("unit_price_distance", "id")
)
