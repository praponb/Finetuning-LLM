from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("qty") < 299))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.min("qty").alias("min_move_qty"))
)

summarized_rows = (
    scoped.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.min_move_qty")
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
)

joined_warehouse = (
    with_activity.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("move_count"),
    )
)

with_counts = (
    joined_warehouse.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

result = (
    with_counts
    .filter(F.col("warehouse_wh_code").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "moved_at_dayofweek",
        "min_move_qty",
        "warehouse_wh_code",
        "warehouse_opened_on",
        "warehouse_city",
        "last_moved_at",
        "move_count",
        F.date_format(F.col("warehouse_opened_on"), "yyyy-MM-dd").alias("warehouse_opened_on_label"),
        F.round((F.unix_timestamp(F.col("last_moved_at")) - F.unix_timestamp(F.col("moved_at"))) / 3600, 1).alias("hours_moved_at_to_last_moved_at"),
    )
    .orderBy("qty")
)
