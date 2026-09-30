from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.col("unit_price") <= 361.9) | (F.col("warehouse_id") >= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
        F.year(F.col("moved_at")).alias("moved_at_year"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.max("qty").alias("max_move_qty"),
    )
)

with_counts = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.last_moved_at", "st.max_move_qty")
)

annotated = (
    with_counts
    .filter(F.col("qty") < 422)
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "moved_at_year",
        "operator_prefix",
        "last_moved_at",
        "max_move_qty",
        F.when(F.col("move_type").isin("ADJUST", "INBOUND"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_moved_at")).alias("days_since_last_moved_at"),
    )
)

result = (
    annotated
    .groupingSets(
        [["sku_id", "moved_at_year"], ["sku_id"], ["moved_at_year"], []],
        "sku_id", "moved_at_year",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count(F.when(F.col("move_type") == "OUTBOUND", 1)).alias("n_move_type_outbound"),
        F.sum("max_move_qty").alias("total_max_move_qty"),
    )
)
