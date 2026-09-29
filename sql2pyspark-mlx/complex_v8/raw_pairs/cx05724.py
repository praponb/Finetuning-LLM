from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter((F.col("move_type").isin("TRANSFER", "OUTBOUND")) & (F.col("qty") > 208))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.wh_code").isNotNull()), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_adj",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

sku_id_stats = (
    joined
    .groupBy("sku_id")
    .agg(F.avg("unit_price_adj").alias("avg_unit_price_adj"))
)

above_average = (
    joined.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.unit_price_adj") > F.col("g.avg_unit_price_adj"))
    .select(
        "s.*",
        "g.avg_unit_price_adj",
        F.round(F.col("s.unit_price_adj") - F.col("g.avg_unit_price_adj"), 2).alias("unit_price_adj_minus_avg"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("qty").alias("min_move_qty"),
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
        F.count("*").alias("move_count"),
    )
)

with_counts = (
    above_average.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.min_move_qty",
        "st.avg_move_unit_price",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

with_parent = (
    with_counts.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.warehouse_capacity",
        "s.warehouse_wh_code",
        "s.avg_unit_price_adj",
        "s.avg_move_unit_price",
        "s.move_count",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.labels").alias("sku_labels"),
        "sk.sku_code",
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(~F.col("operator").isin("op_eve", "op_dew"), 1)).alias("move_operator_count"),
    )
)

with_stats = (
    with_parent.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

with_children_moves = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.move_count",
        "s.sku_is_active",
        "s.sku_code",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

result = (
    with_children_moves
    .select(
        "id",
        "warehouse_id",
        "operator",
        "unit_price",
        "move_id",
        "move_qty",
        "move_move_type",
        F.lower(F.regexp_replace(F.trim(F.col("move_move_type")), " ", "_")).alias("move_move_type_slug"),
    )
    .orderBy("move_id")
)
