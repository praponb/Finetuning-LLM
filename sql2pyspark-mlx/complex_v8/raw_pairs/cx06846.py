from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("unit_price") > 160.8))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.when(F.col("move_type").isin("TRANSFER", "INBOUND"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
        F.when(F.col("move_type").isin("INBOUND", "ADJUST"), "group_a").when(F.col("move_type") == "TRANSFER", "group_b").otherwise("other").alias("move_type_group_2"),
    )
)

with_activity = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") < 273.5) & (F.col("st.operator").isin("op_chan", "op_bee", "op_dew")), "left_semi")
)

with_activity_2 = (
    with_activity.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.city") != "Bangkok") & (F.col("w.capacity") > 4912), "left_anti")
)

result = (
    with_activity_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price_adj",
        "move_type_group_2",
        F.when(F.col("unit_price_adj") >= 412.9, "large").when(F.col("unit_price_adj") >= 150.8, "medium").otherwise("small").alias("unit_price_adj_tier"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price_adj"), F.lit(0)), 3).alias("qty_per_unit_price_adj"),
    )
    .orderBy(F.col("unit_price_adj").asc_nulls_last())
)
