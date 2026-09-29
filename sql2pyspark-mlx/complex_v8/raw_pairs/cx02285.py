from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("qty").between(124, 463)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.concat_ws("|", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

filtered = (
    recent.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isin("op_bee", "op_ann")), "left_semi")
    .filter(F.col("operator_move_type_label").isNotNull())
)

result = (
    filtered
    .groupingSets(
        [["operator", "move_type"], ["operator"], []],
        "operator", "move_type",
    )
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
    .orderBy(F.col("operator").asc_nulls_last(), F.col("move_type").asc_nulls_last())
)
