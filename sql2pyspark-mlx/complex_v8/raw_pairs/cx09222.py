from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.col("sku_id") <= 19) & (F.col("warehouse_id") <= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.moved_at_plus_7d",
        "s.least_qty_unit_price",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

result = (
    with_details
    .groupingSets(
        [["warehouse_id", "sku_id"], ["warehouse_id"], []],
        "warehouse_id", "sku_id",
    )
    .agg(
        F.round(F.avg("least_qty_unit_price"), 2).alias("avg_least_qty_unit_price"),
        F.min("moved_at_plus_7d").alias("first_moved_at_plus_7d"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
