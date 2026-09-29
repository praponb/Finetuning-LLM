from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_all_by_moved_at_hour_moving = Window.orderBy("moved_at_hour", "id").rowsBetween(-6, Window.currentRow)
w_sku_id_warehouse_id_by_qty_f = Window.partitionBy("sku_id", "warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_hour_ties = Window.orderBy("moved_at_hour")
w_warehouse_id_by_qty_prior_3 = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), "id").rowsBetween(-3, -1)
w_sku_id_by_moved_at_moving_1 = Window.partitionBy("sku_id").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_all_by_moved_at_hour = Window.orderBy("moved_at_hour", "id")
w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy("moved_at", "id")
w_distinct_warehouse_id_in_sku = Window.partitionBy("distinct_warehouse_id_in_sku_id", "warehouse_id").orderBy(F.col("unit_price").desc())
w_distinct_warehouse_id_in_sku_2 = Window.partitionBy("distinct_warehouse_id_in_sku_id", "warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")

filtered_moves = (
    df_stock_moves
    .filter(F.col("unit_price") < 323.9)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
    )
)

with_windows = (
    filtered_moves
    .select(
        "*",
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.max("unit_price").over(w_sku_id).alias("sku_id_max_unit_price"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id), 2).alias("unit_price_vs_sku_id_avg"),
        F.round(F.avg("moved_at_hour").over(w_all_by_moved_at_hour_moving), 2).alias("moved_at_hour_avg_7"),
        F.nth_value("moved_at", 2).over(w_sku_id_warehouse_id_by_qty_f).alias("second_moved_at"),
        F.rank().over(w_all_by_moved_at_hour_ties).alias("moved_at_hour_rank"),
        F.round(F.avg("moved_at_hour").over(w_warehouse_id_by_qty_prior_3), 2).alias("moved_at_hour_avg_3_prior"),
        F.round(F.avg("moved_at_hour").over(w_sku_id_by_moved_at_moving_1), 2).alias("moved_at_hour_avg_2"),
        F.size(F.collect_set("move_type").over(w_warehouse_id_sku_id)).alias("distinct_move_type_in_warehouse_id_sku_id"),
        F.size(F.collect_set("move_type").over(w_sku_id)).alias("distinct_move_type_in_sku_id"),
        F.row_number().over(w_all_by_moved_at_hour).alias("moved_at_hour_seq"),
        F.lag("moved_at").over(w_sku_id_move_type_by_moved_at).alias("prev_moved_at"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.round(F.percent_rank().over(w_distinct_warehouse_id_in_sku), 3).alias("unit_price_percent_rank"),
        F.round(F.cume_dist().over(w_distinct_warehouse_id_in_sku_2), 3).alias("unit_price_cume_dist"),
        F.round(F.col("moved_at_hour_avg_3_prior") / F.nullif(F.sum("moved_at_hour_avg_3_prior").over(w_warehouse_id), F.lit(0)), 4).alias("moved_at_hour_avg_3_prior_share_of_warehouse_id"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "unit_price",
        "moved_at_hour",
        "distinct_warehouse_id_in_sku_id",
        "rows_in_sku_id",
        "sku_id_max_unit_price",
        "moved_at_hour_avg_7",
        "moved_at_hour_rank",
        "moved_at_hour_avg_3_prior",
        "moved_at_hour_avg_2",
        "distinct_move_type_in_warehouse_id_sku_id",
        "distinct_move_type_in_sku_id",
        "moved_at_hour_seq",
        "unit_price_percent_rank",
        "unit_price_cume_dist",
        "moved_at_hour_avg_3_prior_share_of_warehouse_id",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.to_date(F.col("second_moved_at")).alias("second_moved_at_date"),
    )
    .orderBy(F.col("second_moved_at_date").desc(), F.col("moved_at_hour_seq").desc())
)
