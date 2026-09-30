from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter(F.col("unit_price") <= 313.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("TRANSFER", "ADJUST"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.greatest(F.col("qty"), F.col("unit_price")).alias("greatest_qty_unit_price"),
        F.when(F.col("unit_price") >= 298.9, "large").when(F.col("unit_price") >= 111.7, "medium").otherwise("small").alias("unit_price_tier"),
    )
)

derived = (
    valid
    .filter(F.col("greatest_qty_unit_price") > 75.8)
    .select(
        "*",
        F.greatest(F.col("greatest_qty_unit_price"), F.col("unit_price")).alias("greatest_greatest_qty_unit_price_unit_price"),
        F.abs(F.col("greatest_qty_unit_price") - 250.5).alias("greatest_qty_unit_price_distance"),
        F.round(F.col("greatest_qty_unit_price") * 1.5, 2).alias("greatest_qty_unit_price_adj"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.max("qty").alias("max_move_qty"),
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
    )
)

with_counts = (
    derived.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        "st.max_move_qty",
        "st.avg_move_unit_price",
    )
)

grouped = (
    with_counts
    .groupBy(
        "move_type",
        F.to_date("last_moved_at").alias("last_moved_at_day"),
        "move_type_group",
    )
    .agg(
        F.round(F.sum("greatest_qty_unit_price_distance"), 2).alias("total_greatest_qty_unit_price_distance"),
        F.round(F.sum("greatest_qty_unit_price") / F.nullif(F.sum("greatest_qty_unit_price_distance"), F.lit(0)), 3).alias("greatest_qty_unit_price_to_greatest_qty_unit_pri"),
        F.count("*").alias("cnt"),
        F.max("max_move_qty").alias("max_max_move_qty"),
    )
    .filter(F.col("cnt") >= 2)
)

labeled = (
    grouped
    .select(
        "*",
        F.date_add(F.col("last_moved_at_day"), 7).alias("last_moved_at_day_plus_7d"),
    )
)

result = (
    labeled
    .select("move_type_group")
    .distinct()
    .orderBy(F.col("move_type_group").desc())
)
