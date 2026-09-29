from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

sku_id_stats = (
    recent_moves
    .groupBy("sku_id")
    .agg(F.avg("qty").alias("avg_qty"))
)

vs_group = (
    recent_moves.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.qty") > F.col("g.avg_qty"))
    .select(
        "s.*",
        "g.avg_qty",
        F.round(F.col("s.qty") - F.col("g.avg_qty"), 2).alias("qty_minus_avg"),
    )
)

with_details = (
    vs_group.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_slug",
        "s.operator_len",
        "s.avg_qty",
        "s.qty_minus_avg",
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

detail_rows_moves = (
    with_details.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.move_type_slug",
        "s.avg_qty",
        "s.warehouse_opened_on",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    detail_rows_moves
    .groupingSets(
        [["move_id", "operator", "sku_id"], ["move_id"], ["operator"], []],
        "move_id", "operator", "sku_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("move_type") == "TRANSFER", F.col("avg_qty"))).alias("avg_qty_move_type_transfer"),
        F.round(100 * F.count(F.when(F.col("operator") == "op_chan", 1)) / F.count("*"), 1).alias("pct_operator_op_chan"),
    )
    .select(
        "move_id",
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        "sku_id",
        "gid",
        "avg_qty_move_type_transfer",
        "pct_operator_op_chan",
    )
)
