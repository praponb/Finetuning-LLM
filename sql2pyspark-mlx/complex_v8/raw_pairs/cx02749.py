from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter(F.month(F.col("moved_at")).isin(2, 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
    )
)

qualified = (
    prepared.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") < 363.6), "left_semi")
)

move_type_stats = (
    qualified
    .groupBy("move_type")
    .agg(F.percentile_approx("unit_price", 0.5).alias("median_unit_price"))
)

vs_group = (
    qualified.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price") >= F.col("g.median_unit_price"))
    .select(
        "s.*",
        "g.median_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.median_unit_price"), 2).alias("unit_price_minus_median"),
    )
)

with_children = (
    vs_group.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.moved_at_plus_15m",
        "s.median_unit_price",
        "s.unit_price_minus_median",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

combined_details_move = (
    with_children.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.move_sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.moved_at_plus_15m",
        "s.median_unit_price",
        "s.unit_price_minus_median",
        "s.move_id",
        "s.move_qty",
        "s.move_move_type",
        "s.move_sku_id",
        F.col("st.move_type").alias("move_move_type_2"),
        F.col("st.qty").alias("move_qty_2"),
        F.col("st.operator").alias("move_operator"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

result = (
    combined_details_move
    .select(
        "id",
        "move_id",
        "median_unit_price",
        "sku_weight_kg",
        "unit_price_minus_median",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["median_unit_price", "sku_weight_kg", "unit_price_minus_median"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "move_id", "measure")
)
