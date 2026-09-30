from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("qty") <= 268))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

with_parent_warehouse = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.capacity") < 11768)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.qty_filled",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

summary = (
    with_parent_warehouse
    .groupBy("operator", "move_type")
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    summary
    .select("operator", "move_type", "cnt")
    .orderBy(F.col("operator").desc(), "move_type")
)
