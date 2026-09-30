from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_operator_by_qty = Window.partitionBy("move_type", "operator").orderBy("qty", "id")
w_move_type = Window.partitionBy("move_type")
w_operator_warehouse_id_by_qty = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("qty").asc_nulls_last(), "id")
w_move_type_by_qty_full = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_qty = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), "id")
w_operator_warehouse_id_by_qty_2 = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_qty_moving_3 = Window.orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_qty = Window.orderBy(F.col("qty").asc_nulls_last(), "id")
w_move_type_operator_by_qty_mo = Window.partitionBy("move_type", "operator").orderBy("qty", "id").rowsBetween(-2, Window.currentRow)
w_warehouse_id_by_qty_ties = Window.partitionBy("warehouse_id").orderBy(F.col("qty").asc_nulls_last())
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_all_by_qty_dense_rank_2_ties = Window.orderBy(F.col("qty_dense_rank_2").asc_nulls_last())
w_all_by_qty_dense_rank_2 = Window.orderBy(F.col("qty_dense_rank_2").asc_nulls_last(), "id")
w_all_by_qty_avg_4 = Window.orderBy(F.col("qty_avg_4").asc_nulls_last(), "id")

prepared = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.ntile(3).over(w_move_type_operator_by_qty).alias("qty_tercile"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_move_type), F.lit(0)), 4).alias("qty_share_of_move_type"),
        F.dense_rank().over(w_operator_warehouse_id_by_qty).alias("qty_dense_rank"),
        F.nth_value("qty", 2).over(w_move_type_by_qty_full).alias("second_qty"),
        F.lead("qty", 2).over(w_move_type_by_qty).alias("next2_qty"),
        F.max("qty").over(w_operator_warehouse_id_by_qty_2).alias("qty_max_4"),
        F.round(F.avg("qty").over(w_all_by_qty_moving_3), 2).alias("qty_avg_4"),
        F.row_number().over(w_all_by_qty).alias("seq_no"),
        F.lead("qty").over(w_operator_warehouse_id_by_qty).alias("next_qty"),
        F.round(F.avg("qty").over(w_move_type_operator_by_qty_mo), 2).alias("qty_avg_3"),
        F.dense_rank().over(w_warehouse_id_by_qty_ties).alias("qty_dense_rank_2"),
        F.round(F.col("qty") - F.avg("qty").over(w_move_type), 2).alias("qty_vs_move_type_avg"),
        F.count(F.lit(1)).over(w_warehouse_id_sku_id).alias("rows_in_warehouse_id_sku_id"),
    )
)

first_rows = with_windows.filter(F.col("qty_dense_rank") <= 5)

windowed = (
    first_rows
    .select(
        "*",
        F.rank().over(w_all_by_qty_dense_rank_2_ties).alias("qty_dense_rank_2_rank"),
        F.max("qty_vs_move_type_avg").over(w_move_type).alias("move_type_max_qty_vs_move_type_avg"),
        F.row_number().over(w_all_by_qty_dense_rank_2).alias("rn"),
        F.ntile(3).over(w_all_by_qty_avg_4).alias("qty_avg_4_tercile"),
    )
)

result = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "qty_tercile",
        "qty_share_of_move_type",
        "qty_dense_rank",
        "second_qty",
        "next2_qty",
        "qty_max_4",
        "qty_avg_4",
        "seq_no",
        "qty_dense_rank_2",
        "qty_vs_move_type_avg",
        "rows_in_warehouse_id_sku_id",
        "rn",
        "qty_avg_4_tercile",
        F.round(F.col("qty_tercile") / F.nullif(F.col("qty_avg_3"), F.lit(0)), 3).alias("qty_tercile_per_qty_avg_3"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
    .orderBy(F.col("qty_tercile").desc(), F.col("operator").desc_nulls_first())
)
