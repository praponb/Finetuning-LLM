from pyspark.sql import functions as F

prepared_moves = df_stock_moves.filter(F.col("warehouse_id").isNotNull())

joined = (
    prepared_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.round(F.avg("qty"), 2).alias("avg_move_qty"),
        F.max("moved_at").alias("last_moved_at"),
        F.max("moved_at").alias("last_moved_at_2"),
    )
)

profiled = (
    joined.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.avg_move_qty",
        "st.last_moved_at",
        "st.last_moved_at_2",
    )
)

line_level = (
    profiled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.sku_is_active",
        "s.sku_unit_cost",
        "s.avg_move_qty",
        "s.last_moved_at",
        "s.last_moved_at_2",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.qty").alias("move_qty"),
    )
)

result = (
    line_level
    .rollup("warehouse_id", "sku_id")
    .agg(
        F.grouping("warehouse_id").alias("is_total_warehouse_id"),
        F.percentile_approx("avg_move_qty", 0.5).alias("median_avg_move_qty"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
