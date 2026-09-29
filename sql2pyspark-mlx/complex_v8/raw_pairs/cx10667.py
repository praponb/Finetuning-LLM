from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_running = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_operator_by_moved_at = Window.partitionBy("sku_id", "operator").orderBy("moved_at", "id")
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_sku_id_operator_by_moved_at_2 = Window.partitionBy("sku_id", "operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())
w_all_by_distinct_warehouse_id = Window.orderBy(F.col("distinct_warehouse_id_in_sku_id_operator").desc(), "id")
w_rn_by_qty_dense_rank_ties = Window.partitionBy("rn").orderBy("qty_dense_rank")
w_all_by_distinct_warehouse_id_2 = Window.orderBy(F.col("distinct_warehouse_id_in_sku_id_operator").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id = Window.partitionBy("sku_id")

base = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_bee", "op_chan"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

windowed = (
    base
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.first("moved_at", ignorenulls=True).over(w_all_by_qty_running).alias("first_known_moved_at"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id_operator)).alias("distinct_warehouse_id_in_sku_id_operator"),
        F.ntile(10).over(w_sku_id_by_moved_at).alias("moved_at_decile"),
        F.rank().over(w_sku_id_operator_by_moved_at).alias("moved_at_rank"),
        F.min("qty").over(w_all_by_qty_running).alias("min_qty_so_far"),
        F.sum("qty").over(w_sku_id_operator).alias("sku_id_operator_sum_qty"),
        F.row_number().over(w_all_by_qty).alias("rn"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_id_operator), F.lit(0)), 4).alias("qty_share_of_sku_id_operator"),
        F.last("qty", ignorenulls=True).over(w_sku_id_operator_by_moved_at_2).alias("last_known_qty"),
        F.min("qty").over(w_sku_id_operator).alias("sku_id_operator_min_qty"),
        F.dense_rank().over(w_all_by_qty_ties).alias("qty_dense_rank"),
    )
)

top_rows = (
    windowed
    .filter(F.col("moved_at_rank") <= 2)
    .drop("moved_at_rank")
)

with_windows = (
    top_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "distinct_warehouse_id_in_sku_id_operator",
        "min_qty_so_far",
        "sku_id_operator_sum_qty",
        "rn",
        "qty_share_of_sku_id_operator",
        "last_known_qty",
        "qty_dense_rank",
        F.lag("moved_at").over(w_all_by_distinct_warehouse_id).alias("prev_moved_at"),
        F.dense_rank().over(w_rn_by_qty_dense_rank_ties).alias("qty_dense_rank_dense_rank"),
        F.row_number().over(w_all_by_distinct_warehouse_id).alias("distinct_warehouse_id_in_sku_id_operator_seq"),
        F.nth_value("qty_share_of_sku_id_operator", 2).over(w_all_by_distinct_warehouse_id_2).alias("second_qty_share_of_sku_id_operator"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "distinct_warehouse_id_in_sku_id_operator",
        "sku_id_operator_sum_qty",
        "rn",
        "qty_share_of_sku_id_operator",
        "qty_dense_rank",
        "prev_moved_at",
        "distinct_warehouse_id_in_sku_id_operator_seq",
        "second_qty_share_of_sku_id_operator",
        "rows_in_sku_id",
        F.date_trunc("quarter", F.col("prev_moved_at")).alias("prev_moved_at_quarter"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("prev_moved_at")).alias("days_since_prev_moved_at"),
    )
    .orderBy(
        F.col("distinct_warehouse_id_in_sku_id_operator_seq").desc(),
        F.col("rows_in_sku_id").asc_nulls_last(),
    )
)
