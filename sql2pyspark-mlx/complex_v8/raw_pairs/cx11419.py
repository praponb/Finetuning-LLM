from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")

base_moves = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(1, 2)) & (F.col("move_type").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

with_windows = (
    base_moves
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
        F.round(F.percent_rank().over(w_warehouse_id_by_qty), 3).alias("qty_percent_rank"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["sku_id", "warehouse_id"], ["sku_id"], ["warehouse_id"], []],
        "sku_id", "warehouse_id",
    )
    .agg(F.percentile_approx("unit_price", 0.5).alias("median_unit_price"))
)
