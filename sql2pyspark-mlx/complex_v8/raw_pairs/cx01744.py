from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("operator") == "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
    )
)

enriched = (
    scoped_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.category").alias("sku_category"),
        "sk.sku_code",
    )
)

operator_stats = (
    enriched
    .groupBy("operator")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

outliers = (
    enriched.alias("s")
    .join(operator_stats.alias("g"), F.col("g.operator") == F.col("s.operator"), "inner")
    .filter(F.col("s.unit_price") > F.col("g.avg_unit_price"))
    .select(
        "s.*",
        "g.avg_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.avg_unit_price"), 2).alias("unit_price_minus_avg"),
    )
)

result = (
    outliers
    .groupBy("warehouse_id")
    .pivot("operator", ["op_eve", "op_chan", "op_dew", "op_bee"])
    .agg(F.sum("avg_unit_price"))
    .orderBy("warehouse_id")
)
