from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_by_moved_at_dayofweek = Window.partitionBy("sku_id").orderBy(F.col("moved_at_dayofweek").desc())
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_sku_id_operator_by_qty = Window.partitionBy("sku_id", "operator").orderBy("qty", "id")
w_all_by_moved_at_running = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_moved_at_dayofweek_2 = Window.partitionBy("sku_id").orderBy(F.col("moved_at_dayofweek").desc(), "id")
w_sku_id_by_moved_at_dayofweek_3 = Window.partitionBy("sku_id").orderBy(F.col("moved_at_dayofweek").desc(), "id").rowsBetween(-3, -1)
w_sku_id_by_next3_qty_running = Window.partitionBy("sku_id").orderBy(F.col("next3_qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_next3_qty = Window.orderBy(F.col("next3_qty").desc(), "id")
w_moved_at_to_date_moved_at_da = Window.partitionBy(F.to_date("moved_at"), "moved_at_dayofweek")
w_sku_id_by_next3_qty = Window.partitionBy("sku_id").orderBy(F.col("next3_qty").desc(), "id")

valid_moves = (
    df_stock_moves
    .filter((F.col("qty").between(109, 349)) | (F.col("warehouse_id") >= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

windowed = (
    valid_moves
    .select(
        "*",
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id), 2).alias("unit_price_vs_warehouse_id_avg"),
        F.round(F.percent_rank().over(w_sku_id_by_moved_at_dayofweek), 3).alias("moved_at_dayofweek_percent_rank"),
        F.lead("qty", 3).over(w_all_by_moved_at).alias("next3_qty"),
        F.dense_rank().over(w_sku_id_by_moved_at_dayofweek).alias("moved_at_dayofweek_dense_rank"),
        F.lag("unit_price", 2).over(w_all_by_moved_at).alias("prev2_unit_price"),
        F.round(F.percent_rank().over(w_all_by_moved_at), 3).alias("moved_at_percent_rank"),
        F.round(F.avg("qty").over(w_warehouse_id), 2).alias("warehouse_id_avg_qty"),
        F.row_number().over(w_sku_id_operator_by_qty).alias("qty_seq"),
        F.round(F.avg("unit_price").over(w_warehouse_id), 2).alias("warehouse_id_avg_unit_price"),
        F.max("unit_price").over(w_all_by_moved_at_running).alias("max_unit_price_so_far"),
        F.lead("moved_at_dayofweek", 1, 0).over(w_sku_id_by_moved_at_dayofweek_2).alias("next_moved_at_dayofweek"),
        F.sum("qty").over(w_sku_id_by_moved_at_dayofweek_3).alias("qty_sum_3_prior"),
        F.lag("moved_at").over(w_sku_id_operator_by_qty).alias("prev_moved_at"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_moved_at)).alias("unit_price_change"),
        F.round(F.col("moved_at_dayofweek") - F.avg("moved_at_dayofweek").over(w_warehouse_id), 2).alias("moved_at_dayofweek_vs_warehouse_id_avg"),
        F.round(F.avg("moved_at_dayofweek").over(w_warehouse_id), 2).alias("warehouse_id_avg_moved_at_dayofweek"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "moved_at_dayofweek_percent_rank",
        "moved_at_dayofweek_dense_rank",
        "warehouse_id_avg_qty",
        "qty_seq",
        "warehouse_id_avg_unit_price",
        "max_unit_price_so_far",
        "next_moved_at_dayofweek",
        "qty_sum_3_prior",
        "unit_price_change",
        "moved_at_dayofweek_vs_warehouse_id_avg",
        F.last("prev_moved_at", ignorenulls=True).over(w_sku_id_by_next3_qty_running).alias("last_known_prev_moved_at"),
        F.row_number().over(w_all_by_next3_qty).alias("next3_qty_seq"),
        F.count(F.lit(1)).over(w_moved_at_to_date_moved_at_da).alias("rows_in_moved_at_to_date_moved_at_dayofweek"),
        F.max("qty_sum_3_prior").over(w_moved_at_to_date_moved_at_da).alias("moved_at_to_date_moved_at_dayofweek_max_qty_sum_"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_sku_id_by_next3_qty))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at_dayofweek_percent_rank",
        "moved_at_dayofweek_dense_rank",
        "warehouse_id_avg_qty",
        "qty_seq",
        "warehouse_id_avg_unit_price",
        "max_unit_price_so_far",
        "qty_sum_3_prior",
        "unit_price_change",
        "moved_at_dayofweek_vs_warehouse_id_avg",
        "last_known_prev_moved_at",
        "next3_qty_seq",
        "rows_in_moved_at_to_date_moved_at_dayofweek",
        "minutes_since_prev_moved_at",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.year(F.col("last_known_prev_moved_at")).alias("last_known_prev_moved_at_year"),
    )
    .orderBy(F.col("operator_len").asc_nulls_last())
)
