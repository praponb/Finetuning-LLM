from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("operator") == "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.abs(F.col("unit_price") - 209.4).alias("unit_price_distance"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.round(F.avg("qty"), 2).alias("avg_move_qty"))
)

with_stats = (
    prepared_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.avg_move_qty")
)

screened = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.month(F.col("st.moved_at")).isin(1, 2)) & (F.col("st.unit_price") < 330.0), "left_semi")
    .filter(F.col("qty_per_unit_price") >= 1.0)
)

result = (
    screened
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price",
        "unit_price_distance",
        "moved_at_date",
        "avg_move_qty",
        F.when(F.col("move_type").isin("ADJUST", "INBOUND"), "group_a").when(F.col("move_type") == "TRANSFER", "group_b").otherwise("other").alias("move_type_group"),
    )
    .orderBy("unit_price", F.col("move_type").desc())
)
