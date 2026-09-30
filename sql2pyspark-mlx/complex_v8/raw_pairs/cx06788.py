from pyspark.sql import functions as F

valid = df_stock_moves.filter((F.col("operator") != "op_dew") & (F.col("unit_price") <= 393.3))

enriched = (
    valid.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

move_type_stats = (
    enriched
    .groupBy("move_type")
    .agg(F.percentile_approx("qty", 0.5).alias("median_qty"))
)

vs_group = (
    enriched.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.qty") >= F.col("g.median_qty"))
    .select(
        "s.*",
        "g.median_qty",
        F.round(F.col("s.qty") - F.col("g.median_qty"), 2).alias("qty_minus_median"),
    )
)

with_children = (
    vs_group.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.sku_is_active",
        "s.median_qty",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
)

with_counts = (
    with_children.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.last_moved_at")
)

result = (
    with_counts
    .select(
        "id",
        "move_type",
        "moved_at",
        "sku_is_active",
        "median_qty",
        "move_id",
        "move_qty",
        "move_move_type",
        "last_moved_at",
    )
    .orderBy(F.col("id").desc(), F.col("move_qty").desc())
)
