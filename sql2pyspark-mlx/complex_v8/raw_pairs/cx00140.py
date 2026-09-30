from pyspark.sql import functions as F

base_warehouses = df_warehouses.filter(F.col("opened_on").isNotNull())

with_children_moves = (
    base_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.wh_code",
        "s.city",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

with_children = (
    with_children_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.city",
        "s.move_id",
        "s.move_unit_price",
        "s.move_qty",
        "s.move_operator",
        "s.move_sku_id",
        F.col("st.id").alias("move_id_2"),
        F.col("st.unit_price").alias("move_unit_price_2"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty_2"),
        "st.move_type",
    )
)

result = (
    with_children
    .cube("move_sku_id", "move_id_2", "move_id")
    .agg(
        F.grouping("move_sku_id").alias("is_total_move_sku_id"),
        F.sum(F.when(F.col("move_type") == "OUTBOUND", 1).otherwise(0)).alias("n_move_type_outbound"),
        F.max("move_qty_2").alias("max_move_qty_2"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
    )
    .orderBy(
        F.col("move_sku_id").asc_nulls_last(),
        F.col("move_id_2").asc_nulls_last(),
        F.col("move_id").asc_nulls_last(),
    )
)
