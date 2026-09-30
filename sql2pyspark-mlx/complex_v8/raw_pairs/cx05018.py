from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_city_sku_id_by_qty = Window.partitionBy("warehouse_city", "sku_id").orderBy("qty")
w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")

filtered_moves = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("sku_id") >= 12))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

combined_details = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.rank().over(w_warehouse_city_sku_id_by_qty).alias("qty_rank"),
        F.round(F.col("qty") - F.avg("qty").over(w_move_type_warehouse_id), 2).alias("qty_vs_move_type_warehouse_id_avg"),
    )
)

tagged = (
    with_windows
    .filter(F.col("qty_rank").between(2, 10))
    .select(
        "*",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        (F.floor(F.col("qty_rank") / 2) * 2).alias("qty_rank_bucket"),
    )
)

result = (
    tagged
    .groupBy("sku_is_active", "moved_at_date")
    .pivot("warehouse_city", ["Bangkok", "Singapore"])
    .agg(F.sum("qty_vs_move_type_warehouse_id_avg"))
    .orderBy("sku_is_active", "moved_at_date")
)
