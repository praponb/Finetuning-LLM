from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.col("warehouse_id") >= 4) & (F.col("move_type").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
    )
)

joined_sku = (
    recent.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("unit_price").alias("max_move_unit_price"),
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
    )
)

with_counts = (
    joined_sku.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.max_move_unit_price",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

result = (
    with_counts
    .groupingSets(
        [["move_count", "sku_category", "sku_id"], ["move_count"]],
        "move_count", "sku_category", "sku_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("n_rows"),
        F.sum("max_move_unit_price").alias("total_max_move_unit_price"),
    )
    .select(
        "move_count",
        F.coalesce(F.col("sku_category"), F.lit("ALL")).alias("sku_category"),
        "sku_id",
        "grouping_level",
        "n_rows",
        "total_max_move_unit_price",
    )
    .orderBy(
        F.col("move_count").asc_nulls_last(),
        F.col("sku_category").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
