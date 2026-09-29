from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("operator") == "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

derived = (
    base
    .filter(F.col("sku_id") <= 9)
    .select(
        "*",
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
        F.when(F.col("unit_price") >= 299.1, "A").when(F.col("unit_price") >= 130.6, "B").otherwise("C").alias("unit_price_tier"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator") == "op_dew", 1)).alias("move_operator_count"),
        F.sum("qty").alias("total_move_qty"),
    )
)

profiled = (
    derived.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        "st.total_move_qty",
    )
)

exploded_detail = (
    profiled.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.to_date(F.col("st.moved_at")) < F.lit("2025-02-24").cast("date")), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price_tier",
        "s.move_operator_count",
        "s.total_move_qty",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

detail_rows = (
    exploded_detail.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") > 73.0), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price_tier",
        "s.move_operator_count",
        "s.total_move_qty",
        "s.move_id",
        "s.move_warehouse_id",
        "s.move_moved_at",
        "s.move_move_type",
        F.col("st.id").alias("move_id_2"),
        F.col("st.warehouse_id").alias("move_warehouse_id_2"),
        F.col("st.move_type").alias("move_move_type_2"),
        F.col("st.unit_price").alias("move_unit_price_2"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("operator") != "op_bee", 1)).alias("move_operator_count_2"),
    )
)

summarized_rows = (
    detail_rows.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count_2"), F.lit(0)).alias("move_operator_count_2"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.move_warehouse_id_2"), "inner")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id_2"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price_tier",
        "s.move_operator_count",
        "s.total_move_qty",
        "s.move_id",
        "s.move_warehouse_id",
        "s.move_move_type",
        "s.move_id_2",
        "s.move_warehouse_id_2",
        "s.move_move_type_2",
        "s.move_unit_price_2",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("st.operator").alias("move_operator"),
        "st.move_type",
    )
)

result = (
    combined_details
    .rollup("move_warehouse_id", "warehouse_id")
    .agg(
        F.min("warehouse_capacity").alias("min_warehouse_capacity"),
        F.round(F.sum("warehouse_capacity") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("warehouse_capacity_to_qty_ratio"),
    )
    .orderBy(
        F.col("move_warehouse_id").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
