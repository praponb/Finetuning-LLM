from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((~F.col("move_type").isin("OUTBOUND", "ADJUST")) & (F.col("unit_price") > 130.6))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
    )
)

combined_details = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

grouped = (
    combined_details
    .groupBy("warehouse_id", "operator")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
        F.min("sku_unit_cost").alias("min_sku_unit_cost"),
        F.max("sku_unit_cost").alias("max_sku_unit_cost"),
    )
    .filter(F.col("n_rows") >= 3)
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.min("moved_at").alias("first_moved_at"))
)

with_stats = (
    grouped.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.first_moved_at")
)

with_parent = (
    with_stats.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.warehouse_id",
        "s.operator",
        "s.last_moved_at",
        "s.distinct_operators",
        "s.n_rows",
        "s.sku_code_list",
        "s.min_sku_unit_cost",
        "s.first_moved_at",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

with_details_warehouse = (
    with_parent.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity_2"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

combined_details_2 = (
    with_details_warehouse.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on_2"),
        F.col("w.city").alias("warehouse_city_2"),
    )
)

result = (
    combined_details_2
    .select(
        "warehouse_id",
        "operator",
        "distinct_operators",
        "sku_code_list",
        "min_sku_unit_cost",
        "first_moved_at",
        "warehouse_opened_on",
        "warehouse_city",
        "warehouse_city_2",
        F.array_join(F.array_sort(F.col("sku_code_list")), ",").alias("sku_code_list_text"),
        F.when(F.col("operator").isin("op_eve", "op_chan"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
    )
)
