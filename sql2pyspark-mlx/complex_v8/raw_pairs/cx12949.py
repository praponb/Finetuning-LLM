from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_operator_by_moved_at_full = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_qty_running = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_qty_full = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_qty_ties = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc())
w_warehouse_id_by_qty_moving_6 = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(-6, Window.currentRow)
w_qty_dense_rank_sku_id_by_qty = Window.partitionBy("qty_dense_rank", "sku_id").orderBy("qty_sum_7", "id")
w_qty_dense_rank_sku_id_by_qty_2 = Window.partitionBy("qty_dense_rank", "sku_id").orderBy("qty_sum_7", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_qty_dense_rank_warehouse_id = Window.partitionBy("qty_dense_rank", "warehouse_id")
w_qty_dense_rank_sku_id_by_war = Window.partitionBy("qty_dense_rank", "sku_id").orderBy(F.col("warehouse_id_sum_qty").desc(), "id")

recent_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

windowed = (
    recent_moves
    .select(
        "*",
        F.min("qty").over(w_operator).alias("operator_min_qty"),
        F.sum("qty").over(w_warehouse_id).alias("warehouse_id_sum_qty"),
        F.nth_value("moved_at", 2).over(w_operator_by_moved_at_full).alias("second_moved_at"),
        F.max("qty").over(w_warehouse_id).alias("warehouse_id_max_qty"),
        F.last("qty", ignorenulls=True).over(w_warehouse_id_by_qty_running).alias("last_known_qty"),
        F.first("qty").over(w_warehouse_id_by_qty_full).alias("first_qty"),
        F.dense_rank().over(w_warehouse_id_by_qty_ties).alias("qty_dense_rank"),
        F.sum("qty").over(w_warehouse_id_by_qty_moving_6).alias("qty_sum_7"),
    )
)

first_rows = windowed.filter((F.col("qty_dense_rank") <= 5) & (F.col("first_qty") >= 189))

with_windows = (
    first_rows
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "operator",
        "operator_min_qty",
        "second_moved_at",
        "last_known_qty",
        "first_qty",
        "qty_sum_7",
        F.ntile(10).over(w_qty_dense_rank_sku_id_by_qty).alias("qty_sum_7_decile"),
        F.count(F.lit(1)).over(w_qty_dense_rank_sku_id_by_qty_2).alias("running_count"),
        F.size(F.collect_set("operator").over(w_qty_dense_rank_warehouse_id)).alias("distinct_operator_in_qty_dense_rank_warehouse_id"),
        (F.col("qty_sum_7") - F.lag("qty_sum_7").over(w_qty_dense_rank_sku_id_by_war)).alias("qty_sum_7_change"),
        F.lead("first_qty", 2).over(w_qty_dense_rank_sku_id_by_qty).alias("next2_first_qty"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "operator_min_qty",
        "second_moved_at",
        "first_qty",
        "qty_sum_7",
        "running_count",
        "distinct_operator_in_qty_dense_rank_warehouse_id",
        "qty_sum_7_change",
        "next2_first_qty",
        F.when(F.col("operator_min_qty") >= 3500, "A").when(F.col("operator_min_qty") >= 1751, "B").otherwise("C").alias("operator_min_qty_tier"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
    .orderBy("operator_len")
)
