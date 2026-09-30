from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_all_by_qty_moving_3 = Window.orderBy("qty", "id").rowsBetween(-3, Window.currentRow)
w_all_by_moved_at_year = Window.orderBy(F.col("moved_at_year").desc(), "id")
w_all_by_moved_at_year_running = Window.orderBy(F.col("moved_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_year_full = Window.orderBy(F.col("moved_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type = Window.partitionBy("move_type")

valid = (
    df_stock_moves
    .filter(F.col("sku_id") <= 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
    )
)

with_activity = (
    valid.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.month(F.col("st.moved_at")).isin(1, 2)), "left_anti")
    .filter(F.col("operator").isin("op_ann", "op_bee"))
)

with_windows = (
    with_activity
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "moved_at_year",
        "operator_prefix",
        F.dense_rank().over(w_all_by_qty).alias("qty_dense_rank"),
        F.round(F.avg("qty").over(w_all_by_qty_moving_3), 2).alias("qty_avg_4"),
        F.round(F.cume_dist().over(w_all_by_moved_at_year), 3).alias("moved_at_year_cume_dist"),
        F.first("unit_price", ignorenulls=True).over(w_all_by_moved_at_year_running).alias("first_known_unit_price"),
        F.last("moved_at").over(w_all_by_moved_at_year_full).alias("last_moved_at"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
    )
)

filtered = (
    with_windows
    .crossJoin(with_windows.agg(F.min("qty_avg_4").alias("overall_min_qty_avg_4")))
    .filter(F.col("qty_avg_4") > F.col("overall_min_qty_avg_4"))
    .drop("overall_min_qty_avg_4")
)

top_rows = (
    filtered
    .filter(F.col("qty_dense_rank") <= 5)
    .drop("qty_dense_rank")
)

result = (
    top_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at_year",
        "operator_prefix",
        "qty_avg_4",
        "moved_at_year_cume_dist",
        "first_known_unit_price",
        "last_moved_at",
        "rows_in_move_type",
    )
    .orderBy("operator_prefix", F.col("qty").desc())
)
