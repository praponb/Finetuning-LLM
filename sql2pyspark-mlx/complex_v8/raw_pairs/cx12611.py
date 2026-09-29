from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter(F.col("qty") >= 112)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

labeled = (
    recent
    .filter(F.col("warehouse_id") <= 4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        F.concat_ws("-", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

screened = (
    labeled.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") < 299.1), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
    )
    .orderBy(F.col("sku_id").asc_nulls_last(), F.col("moved_at").desc_nulls_first())
)
