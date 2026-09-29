from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("unit_price").between(73.0, 313.8)) & (F.col("qty") > 145))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
    )
)

enriched_warehouse = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_filled",
        "s.moved_at_plus_30m",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

per_group = (
    enriched_warehouse
    .filter(F.year(F.col("warehouse_opened_on")) == 2024)
    .groupBy(
        F.date_format("moved_at_plus_30m", "yyyy-MM").alias("moved_at_plus_30m_ym"),
        "sku_id",
    )
    .agg(
        F.max("moved_at_plus_30m").alias("last_moved_at_plus_30m"),
        F.sum(F.when(F.col("operator") == "op_ann", 1).otherwise(0)).alias("n_operator_op_ann"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.min("moved_at").alias("first_moved_at"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.percentile_approx("unit_price_filled", 0.5).alias("median_unit_price_filled"),
    )
)

with_children_moves = (
    per_group.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id").isNotNull()), "inner")
    .select(
        "s.moved_at_plus_30m_ym",
        "s.sku_id",
        "s.last_moved_at_plus_30m",
        "s.n_operator_op_ann",
        "s.distinct_move_types",
        "s.median_unit_price_filled",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        "st.move_type",
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    with_children_moves
    .select(
        "moved_at_plus_30m_ym",
        "sku_id",
        "distinct_move_types",
        "median_unit_price_filled",
        "move_id",
        "move_unit_price",
        "move_type",
        "move_warehouse_id",
        "move_moved_at",
    )
    .orderBy(F.col("distinct_move_types").asc_nulls_last(), "sku_id")
)
