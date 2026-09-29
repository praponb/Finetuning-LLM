from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) | (F.col("sku_id") >= 8))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("move_count"),
    )
)

with_stats = (
    scoped.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        "st.first_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

enriched = (
    with_stats.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.weight_kg") <= 14.6), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.operator_move_type_label",
        "s.move_type_prefix",
        "s.last_moved_at",
        "s.first_moved_at",
        "s.move_count",
        "sk.sku_code",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty").between(102, 422)) & (F.col("st.sku_id") <= 10), "left_anti")
)

with_parent = (
    filtered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.move_type_prefix",
        "s.last_moved_at",
        "s.first_moved_at",
        "s.move_count",
        "s.sku_code",
        "s.warehouse_wh_code",
        F.col("sk.sku_code").alias("sku_sku_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "move_type_prefix",
        "last_moved_at",
        "move_count",
        "warehouse_wh_code",
        "sku_sku_code",
        F.substring(F.trim(F.col("move_type_prefix")), 1, 2).alias("move_type_prefix_prefix"),
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
    .orderBy(F.col("move_type").asc_nulls_last(), "id")
    .limit(20)
)
