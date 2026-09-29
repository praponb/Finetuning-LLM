from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_all_by_moved_at_running = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_moved_at_ties = Window.orderBy("moved_at")

prepared_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") >= 2) & (F.hour(F.col("moved_at")).between(8, 14)))
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

windowed = (
    prepared_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") - F.lag("qty").over(w_all_by_moved_at)).alias("qty_change"),
        F.row_number().over(w_all_by_moved_at).alias("moved_at_seq"),
        F.last("unit_price", ignorenulls=True).over(w_all_by_moved_at_running).alias("last_known_unit_price"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id), F.lit(0)), 4).alias("qty_share_of_warehouse_id"),
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank"),
    )
)

result = (
    windowed
    .groupingSets(
        [["moved_at_seq", "sku_id"], ["moved_at_seq"], []],
        "moved_at_seq", "sku_id",
    )
    .agg(F.count("*").alias("cnt"))
)
