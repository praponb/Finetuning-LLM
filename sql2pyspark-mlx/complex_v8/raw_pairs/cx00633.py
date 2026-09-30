from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_operator_len_by_unit_price_r = Window.partitionBy("operator_len").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_operator_len_ties = Window.orderBy("operator_len")
w_sku_id_by_qty_ties = Window.partitionBy("sku_id").orderBy(F.col("qty").desc())
w_sku_id_by_qty = Window.partitionBy("sku_id").orderBy(F.col("qty").desc(), "id")
w_sku_id_by_qty_running = Window.partitionBy("sku_id").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_qty_full = Window.partitionBy("sku_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_qty_2 = Window.partitionBy("sku_id").orderBy("qty", "id")
w_seq_no_warehouse_id = Window.partitionBy("seq_no", "warehouse_id")
w_sku_id_by_seq_no_moving_2 = Window.partitionBy("sku_id").orderBy("seq_no", "id").rowsBetween(-2, Window.currentRow)
w_sku_id_by_seq_no = Window.partitionBy("sku_id").orderBy("seq_no", "id")
w_all_by_operator_len = Window.orderBy(F.col("operator_len").asc_nulls_last(), "id")
w_move_type_distinct_warehouse = Window.partitionBy("move_type", "distinct_warehouse_id_in_operator_sku_id")

prepared_moves = (
    df_stock_moves
    .filter((F.col("unit_price") <= 367.7) | (F.col("qty") >= 91))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

ranked = (
    prepared_moves
    .select(
        "*",
        F.round(F.avg("unit_price").over(w_operator_sku_id), 2).alias("operator_sku_id_avg_unit_price"),
        F.sum("unit_price").over(w_operator_len_by_unit_price_r).alias("running_unit_price"),
        F.rank().over(w_all_by_operator_len_ties).alias("operator_len_rank"),
        F.round(F.percent_rank().over(w_sku_id_by_qty_ties), 3).alias("qty_percent_rank"),
        F.row_number().over(w_sku_id_by_qty).alias("seq_no"),
        F.lag("qty").over(w_sku_id_by_qty).alias("prev_qty"),
        F.min("operator_len").over(w_sku_id_by_qty_running).alias("min_operator_len_so_far"),
        F.last("operator_len").over(w_sku_id_by_qty_full).alias("last_operator_len"),
        F.size(F.collect_set("warehouse_id").over(w_operator_sku_id)).alias("distinct_warehouse_id_in_operator_sku_id"),
        F.min("qty").over(w_operator_sku_id).alias("operator_sku_id_min_qty"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_sku_id_by_qty_2))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator_sku_id), F.lit(0)), 4).alias("qty_share_of_operator_sku_id"),
    )
)

top_rows = (
    ranked
    .filter(F.col("operator_len_rank") == 1)
    .drop("operator_len_rank")
)

metrics = (
    top_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "operator_len",
        "running_unit_price",
        "qty_percent_rank",
        "seq_no",
        "prev_qty",
        "min_operator_len_so_far",
        "last_operator_len",
        "operator_sku_id_min_qty",
        "minutes_since_prev_moved_at",
        "qty_share_of_operator_sku_id",
        F.sum("operator_len").over(w_seq_no_warehouse_id).alias("seq_no_warehouse_id_sum_operator_len"),
        F.max("prev_qty").over(w_sku_id_by_seq_no_moving_2).alias("prev_qty_max_3"),
        F.row_number().over(w_sku_id_by_seq_no).alias("seq_no_2"),
        F.row_number().over(w_all_by_operator_len).alias("operator_len_seq"),
        F.sum("operator_len").over(w_move_type_distinct_warehouse).alias("move_type_distinct_warehouse_id_in_operator_sku_"),
        F.count(F.lit(1)).over(w_seq_no_warehouse_id).alias("rows_in_seq_no_warehouse_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "operator",
        "operator_len",
        "running_unit_price",
        "qty_percent_rank",
        "seq_no",
        "prev_qty",
        "min_operator_len_so_far",
        "operator_sku_id_min_qty",
        "minutes_since_prev_moved_at",
        "qty_share_of_operator_sku_id",
        "prev_qty_max_3",
        "seq_no_2",
        "operator_len_seq",
        "move_type_distinct_warehouse_id_in_operator_sku_",
        "rows_in_seq_no_warehouse_id",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
    )
    .orderBy("operator_or_unknown")
)
