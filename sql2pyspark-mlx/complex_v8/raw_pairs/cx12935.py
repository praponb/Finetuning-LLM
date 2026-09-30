from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter((F.col("move_type").isin("TRANSFER", "INBOUND", "ADJUST")) & (F.col("qty") < 415))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
    )
)

grouped = (
    recent_moves
    .groupBy("warehouse_id")
    .agg(
        F.round(100 * F.count(F.when(~F.col("move_type").isin("ADJUST", "INBOUND"), 1)) / F.count("*"), 1).alias("pct_move_type_not_in_adjust_inbo"),
        F.count(F.when(F.col("move_type").isin("ADJUST", "TRANSFER"), 1)).alias("n_move_type_in_adjust_transfer"),
        F.count(F.when(F.col("move_type").isin("OUTBOUND", "ADJUST", "INBOUND"), 1)).alias("n_move_type_in_outbound_adjust"),
    )
)

line_level_moves = (
    grouped.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.warehouse_id",
        "s.n_move_type_in_adjust_transfer",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.qty").alias("move_qty"),
    )
)

without_matches = (
    line_level_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.moved_at") >= F.lit("2025-01-13 10:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("move_type").isNotNull())
)

result = (
    without_matches
    .select(
        "warehouse_id",
        "n_move_type_in_adjust_transfer",
        "move_type",
        "move_qty",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
    )
    .orderBy(F.col("warehouse_id").desc())
)
