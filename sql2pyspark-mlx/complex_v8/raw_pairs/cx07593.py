from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter(F.col("qty") <= 348)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("qty") >= 288, "HIGH").when(F.col("qty") >= 191, "MEDIUM").otherwise("LOW").alias("qty_tier"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

move_type_stats = (
    scoped
    .groupBy("move_type")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

above_average = (
    scoped.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price") == F.col("g.max_unit_price"))
    .select(
        "s.*",
        "g.max_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.max_unit_price"), 2).alias("unit_price_minus_max"),
    )
)

with_activity = (
    above_average.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
    .filter(F.col("unit_price_minus_max") < 400.0)
)

tagged = with_activity.withColumn("unit_price_plus_qty", F.col("unit_price") + F.col("qty"))

result = (
    tagged
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "qty_tier",
        "moved_at_label",
        "max_unit_price",
        "unit_price_minus_max",
        "unit_price_plus_qty",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
        F.when(F.col("operator").isin("op_bee", "op_eve"), "group_a").when(F.col("operator") == "op_chan", "group_b").otherwise("other").alias("operator_group"),
    )
)
