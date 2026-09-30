from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(7, 12)) & (F.col("move_type").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

with_counts = (
    cleaned_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.sum("unit_price").alias("total_move_unit_price"),
        F.count("*").alias("move_count_2"),
    )
)

with_counts_2 = (
    with_counts.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.total_move_unit_price",
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

result = (
    with_counts_2
    .groupingSets(
        [["sku_id", "move_count"], ["sku_id"], []],
        "sku_id", "move_count",
    )
    .agg(
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.round(F.sum("move_count_2") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("move_count_2_to_qty_ratio"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("sku_id").asc_nulls_last(), F.col("move_count").asc_nulls_last())
)
