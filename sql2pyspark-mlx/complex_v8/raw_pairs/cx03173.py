from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter((F.col("unit_price") < 328.3) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.month(F.col("moved_at")).alias("moved_at_month"),
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.least_unit_price_qty",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

warehouse_city_stats = (
    enriched
    .groupBy("warehouse_city")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

vs_group = (
    enriched.alias("s")
    .join(warehouse_city_stats.alias("g"), F.col("g.warehouse_city") == F.col("s.warehouse_city"), "inner")
    .filter(F.col("s.unit_price") > F.col("g.avg_unit_price"))
    .select(
        "s.*",
        "g.avg_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.avg_unit_price"), 2).alias("unit_price_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "least_unit_price_qty",
        "avg_unit_price",
        "unit_price_minus_avg",
    )
    .orderBy("least_unit_price_qty")
)
