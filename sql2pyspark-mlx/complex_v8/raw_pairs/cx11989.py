from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter((F.col("operator") == "op_ann") & (F.hour(F.col("moved_at")).between(6, 12)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

labeled = (
    scoped_moves
    .filter(F.col("qty") <= 393)
    .select(
        "*",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

labeled_2 = (
    labeled
    .filter(F.col("moved_at_date").isNotNull())
    .select(
        "*",
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.concat(F.col("operator"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("operator_ref"),
    )
)

line_level = (
    labeled_2.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.moved_at_hour",
        "s.qty_minus_unit_price",
        "s.unit_price_plus_qty",
        "s.unit_price_adj",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

screened = (
    line_level
    .crossJoin(line_level.agg(F.avg("unit_price_plus_qty").alias("overall_avg_unit_price_plus_qty")))
    .filter(F.col("unit_price_plus_qty") > F.col("overall_avg_unit_price_plus_qty"))
    .drop("overall_avg_unit_price_plus_qty")
)

screened_2 = (
    screened.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator") == "op_dew"), "left_semi")
)

warehouse_id_stats = (
    screened_2
    .groupBy("warehouse_id")
    .agg(F.avg("qty_minus_unit_price").alias("avg_qty_minus_unit_price"))
)

vs_group = (
    screened_2.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.qty_minus_unit_price") > F.col("g.avg_qty_minus_unit_price"))
    .select(
        "s.*",
        "g.avg_qty_minus_unit_price",
        F.round(F.col("s.qty_minus_unit_price") - F.col("g.avg_qty_minus_unit_price"), 2).alias("qty_minus_unit_price_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        "qty_minus_unit_price",
        "unit_price_plus_qty",
        "unit_price_adj",
        "move_operator",
        "move_moved_at",
        "avg_qty_minus_unit_price",
        "qty_minus_unit_price_minus_avg",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
        F.dayofweek(F.col("move_moved_at")).alias("move_moved_at_dayofweek"),
    )
    .orderBy("unit_price")
)
