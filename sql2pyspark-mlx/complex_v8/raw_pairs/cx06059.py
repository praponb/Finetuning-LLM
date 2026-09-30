from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter((F.col("operator") == "op_chan") & (F.month(F.col("moved_at")).isin(1, 3)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

line_level_moves = (
    scoped_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isin("op_chan", "op_dew", "op_bee")), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
        "st.move_type",
    )
)

stats = (
    line_level_moves
    .groupBy("move_operator", "sku_id")
    .agg(
        F.percentile_approx("move_unit_price", 0.5).alias("median_move_unit_price"),
        F.round(F.sum("move_unit_price"), 2).alias("total_move_unit_price"),
        F.round(F.avg("move_unit_price"), 2).alias("avg_move_unit_price"),
    )
)

without_matches = (
    stats.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isin("op_chan", "op_ann", "op_eve")) & (F.col("st.moved_at") < F.lit("2025-02-18 15:00:00").cast("timestamp")), "left_anti")
)

joined_sku = (
    without_matches.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category").isin("Electronics", "Food")), "inner")
    .select(
        "s.*",
        F.col("sk.labels").alias("sku_labels"),
        "sk.sku_code",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

without_matches_2 = (
    joined_sku.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") <= 330.0) & (F.col("st.move_type") == "OUTBOUND"), "left_semi")
    .filter(F.col("avg_move_unit_price") <= 350.3)
)

result = (
    without_matches_2
    .select(
        "move_operator",
        "sku_id",
        "median_move_unit_price",
        "total_move_unit_price",
        "sku_labels",
        "sku_code",
        "sku_weight_kg",
    )
    .orderBy(
        F.col("median_move_unit_price").asc_nulls_last(),
        F.col("sku_code").desc(),
    )
)
