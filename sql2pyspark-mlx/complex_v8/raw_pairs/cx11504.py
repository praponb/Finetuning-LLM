from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 5) & (F.hour(F.col("moved_at")).between(7, 13)))
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

tagged = (
    cleaned
    .filter(F.col("qty") >= 124)
    .select(
        "*",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

detail_rows = (
    tagged.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
    )
)

result = (
    detail_rows
    .groupingSets(
        [["move_type", "move_operator", "operator"], ["move_type"], ["move_operator"], []],
        "move_type", "move_operator", "operator",
    )
    .agg(
        F.grouping("move_type").alias("is_total_move_type"),
        F.array_sort(F.collect_set("move_operator")).alias("move_operator_list"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.sum(F.when(F.col("move_type") != "OUTBOUND", 1).otherwise(0)).alias("n_move_type_outbound"),
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("move_operator").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
    )
)
