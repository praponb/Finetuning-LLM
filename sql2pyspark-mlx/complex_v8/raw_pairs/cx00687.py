from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter(F.col("qty") <= 300)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
    )
)

with_details = (
    recent_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    with_details
    .groupingSets(
        [["warehouse_id", "sku_id"], ["warehouse_id"], ["sku_id"]],
        "warehouse_id", "sku_id",
    )
    .agg(F.sum("sku_unit_cost").alias("total_sku_unit_cost"))
)
