from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter(F.col("move_type") != "ADJUST")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

combined_details_warehouse = (
    cleaned_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

filtered = (
    combined_details_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

sku_id_stats = (
    filtered
    .groupBy("sku_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

vs_group = (
    filtered.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.unit_price") > F.col("g.avg_unit_price"))
    .select(
        "s.*",
        "g.avg_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.avg_unit_price"), 2).alias("unit_price_minus_avg"),
    )
)

result = (
    vs_group
    .groupingSets(
        [["operator_or_unknown", "sku_id"], ["operator_or_unknown"], ["sku_id"], []],
        "operator_or_unknown", "sku_id",
    )
    .agg(
        F.grouping("operator_or_unknown").alias("is_total_operator_or_unknown"),
        F.count("*").alias("row_count"),
        F.min("moved_at").alias("first_moved_at"),
        F.countDistinct("operator_or_unknown").alias("distinct_operator_or_unknowns"),
    )
    .orderBy(
        F.col("operator_or_unknown").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
