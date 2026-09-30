from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.concat_ws(" / ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

enriched_sku = (
    filtered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.move_type_operator_label",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.city").alias("warehouse_city"),
    )
)

line_level = (
    enriched_sku.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.sku_unit_cost",
        "s.warehouse_city",
        F.col("st.id").alias("move_id"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

screened = (
    line_level.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.move_sku_id")) & (F.col("st.unit_price") > 206.8) & (F.hour(F.col("st.moved_at")).between(10, 12)), "left_anti")
)

result = (
    screened
    .groupingSets(
        [["move_id", "sku_id"], ["move_id"], []],
        "move_id", "sku_id",
    )
    .agg(
        F.array_sort(F.collect_set("warehouse_city")).alias("warehouse_city_list"),
        F.round(100 * F.count(F.when(~F.col("move_move_type").isin("TRANSFER", "ADJUST"), 1)) / F.count("*"), 1).alias("pct_move_move_type_not_in_transf"),
    )
)
