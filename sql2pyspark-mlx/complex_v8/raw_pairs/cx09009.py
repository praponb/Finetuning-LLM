from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.col("unit_price") > 205.1)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

detail_rows_moves = (
    valid_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") < 326), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.move_type_or_unknown",
        "s.moved_at_quarter",
        "s.unit_price_plus_qty",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

warehouse_id_stats = (
    detail_rows_moves
    .groupBy("warehouse_id")
    .agg(F.avg("qty").alias("avg_qty"))
)

above_average = (
    detail_rows_moves.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.qty") > F.col("g.avg_qty"))
    .select(
        "s.*",
        "g.avg_qty",
        F.round(F.col("s.qty") - F.col("g.avg_qty"), 2).alias("qty_minus_avg"),
    )
)

result = (
    above_average
    .filter(F.col("move_type_or_unknown").isin("OUTBOUND", "UNKNOWN"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "moved_at_quarter",
        "unit_price_plus_qty",
        "move_unit_price",
        "move_qty",
        "move_moved_at",
        "avg_qty",
        "qty_minus_avg",
    )
    .orderBy("unit_price", "moved_at_quarter")
)
