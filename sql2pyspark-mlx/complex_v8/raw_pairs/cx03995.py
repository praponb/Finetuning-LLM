from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("qty") >= 67) & (F.col("operator") != "op_dew"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.1, 2).alias("unit_price_adj"),
        F.when(F.col("operator").isin("op_dew", "op_ann"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj_2"),
    )
)

filtered = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.min("qty").alias("min_move_qty"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.min_move_qty",
        "st.last_moved_at",
    )
)

result = (
    with_stats
    .rollup("sku_id", "operator_group")
    .agg(
        F.count(F.when(F.col("move_type").isNotNull(), 1)).alias("n_move_type_is_not_null"),
    )
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("operator_group").asc_nulls_last(),
    )
)
