from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("unit_price") < 293.2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
    )
)

enriched = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.wh_code").isNotNull()), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.moved_at_quarter",
        "s.unit_price_adj",
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") > 136.0) & (~F.col("st.operator").isin("op_chan", "op_dew")), "left_semi")
)

with_children_moves = (
    with_activity.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.warehouse_city",
        "s.sku_is_active",
        "s.sku_labels",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

result = (
    with_children_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "sku_is_active",
        "sku_labels",
        "move_id",
        "move_operator",
        "move_move_type",
        F.when(F.col("sku_is_active"), 1).otherwise(0).alias("sku_is_active_flag"),
    )
    .orderBy("warehouse_id")
)
