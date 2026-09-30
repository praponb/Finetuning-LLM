from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter((F.col("qty") < 300) & (F.col("unit_price") >= 61.2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.when(F.col("qty") >= 370, "large").when(F.col("qty") >= 169, "medium").otherwise("small").alias("qty_tier"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"))
)

profiled = (
    valid.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.avg_move_unit_price")
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"), F.sum("qty").alias("total_move_qty"))
)

profiled_2 = (
    profiled.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.total_move_qty",
    )
)

with_parent_sku = (
    profiled_2.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.unit_cost") > 158.7), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.days_since_moved_at",
        "s.qty_tier",
        "s.unit_price_filled",
        "s.move_count",
        "s.total_move_qty",
        F.col("sk.category").alias("sku_category"),
    )
)

result = (
    with_parent_sku
    .groupBy("sku_id")
    .pivot("operator", ["op_dew", "op_bee", "op_eve", "op_chan"])
    .agg(F.max("days_since_moved_at"))
    .orderBy("sku_id")
)
