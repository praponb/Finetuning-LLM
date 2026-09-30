from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("unit_price") >= 112.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
        F.when(F.col("operator").isin("op_ann", "op_chan"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

with_children_moves = (
    filtered_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator").isin("op_dew", "op_chan")), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.operator_group",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    with_children_moves
    .groupBy("move_id")
    .pivot("move_type", ["INBOUND", "ADJUST", "TRANSFER", "OUTBOUND"])
    .agg(F.count("move_unit_price"))
    .orderBy("move_id")
)
