from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.col("sku_id") <= 3) & (F.hour(F.col("moved_at")).between(10, 12)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
        F.abs(F.col("qty") - 249).alias("qty_distance"),
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

combined_details = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.qty_distance",
        "sk.sku_code",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

grouped = (
    combined_details
    .groupBy("warehouse_id")
    .agg(
        F.percentile_approx("warehouse_capacity", 0.5).alias("median_warehouse_capacity"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
    )
)

qualified = (
    grouped.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
    .filter(F.size(F.col("sku_code_list")) >= 1)
)

without_matches = (
    qualified.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

result = (
    without_matches
    .select(
        "warehouse_id",
        "median_warehouse_capacity",
        F.array_join(F.array_sort(F.col("sku_code_list")), ",").alias("sku_code_list_text"),
        F.array_join(F.array_sort(F.col("sku_code_list")), ",").alias("sku_code_list_text_2"),
    )
    .orderBy(
        F.col("median_warehouse_capacity").desc(),
        F.col("warehouse_id").asc_nulls_last(),
    )
    .limit(20)
)
