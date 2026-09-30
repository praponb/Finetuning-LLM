from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("qty").between(139, 299)) & (F.hour(F.col("moved_at")).between(7, 11)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
        F.when(F.col("move_type").isin("ADJUST", "TRANSFER"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

combined_details_warehouse = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.sum("unit_price").alias("total_move_unit_price"),
        F.sum("unit_price").alias("total_move_unit_price_2"),
        F.count(F.when(F.col("operator").isin("op_chan", "op_eve"), 1)).alias("move_operator_count"),
    )
)

profiled = (
    combined_details_warehouse.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.total_move_unit_price",
        "st.total_move_unit_price_2",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

result = (
    profiled
    .groupingSets(
        [["move_type_group", "move_operator_count", "sku_id"], ["move_type_group"], ["move_operator_count"], []],
        "move_type_group", "move_operator_count", "sku_id",
    )
    .agg(F.countDistinct("move_type_group").alias("distinct_move_type_groups"))
    .orderBy(
        F.col("move_type_group").asc_nulls_last(),
        F.col("move_operator_count").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
