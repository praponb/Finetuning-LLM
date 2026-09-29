from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("sku_id") >= 18)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("unit_price") - 209.4).alias("unit_price_distance"),
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
    )
)

labeled = filtered_moves.select("*", F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"))

per_group = (
    labeled
    .groupBy(F.year("moved_at").alias("moved_at_year"), "operator", "warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type") == "TRANSFER", 1)).alias("n_move_type_transfer"),
        F.sum("qty").alias("total_qty"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("operator") != "op_chan", F.col("qty"))).alias("qty_operator_op_chan"),
    )
)

with_parent_warehouse = (
    per_group.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.moved_at_year",
        "s.operator",
        "s.warehouse_id",
        "s.n_move_type_transfer",
        "s.total_qty",
        "s.n_rows",
        "s.qty_operator_op_chan",
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

enriched_warehouse = (
    with_parent_warehouse.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.col("w.wh_code").alias("warehouse_wh_code_2"))
)

scored = (
    enriched_warehouse
    .withColumn("warehouse_wh_code_2_slug", F.lower(F.regexp_replace(F.trim(F.col("warehouse_wh_code_2")), " ", "_")))
)

result = (
    scored
    .select(
        "moved_at_year",
        "total_qty",
        "n_rows",
        "qty_operator_op_chan",
        "warehouse_wh_code",
        "warehouse_wh_code_2",
        "warehouse_wh_code_2_slug",
    )
)
