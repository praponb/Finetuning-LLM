from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_all_by_unit_price_full = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty = Window.orderBy(F.col("qty").asc_nulls_last(), "id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_qty_full = Window.orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_move_type_warehouse_i = Window.orderBy(F.col("move_type_warehouse_id_min_qty").desc(), "id").rowsBetween(-1, Window.currentRow)
w_warehouse_id_by_rows_in_move = Window.partitionBy("warehouse_id").orderBy("rows_in_move_type_warehouse_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_all_by_move_type_warehouse_i_2 = Window.orderBy(F.col("move_type_warehouse_id_min_qty").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_rows_in_move_type_war = Window.orderBy("rows_in_move_type_warehouse_id_operator_is_not_n", "warehouse_id", "seq_no", "move_type_warehouse_id_min_qty_dense_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-03-08 12:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.size(F.collect_set("sku_id").over(w_move_type_warehouse_id)).alias("distinct_sku_id_in_move_type_warehouse_id"),
        F.row_number().over(w_sku_id_by_unit_price).alias("seq_no"),
        F.max("qty").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_max_qty"),
        F.min("qty").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_min_qty"),
        F.nth_value("unit_price", 2).over(w_all_by_unit_price_full).alias("second_unit_price"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_move_type_warehouse_id), F.lit(0)), 4).alias("qty_share_of_move_type_warehouse_id"),
        F.round(F.cume_dist().over(w_all_by_qty), 3).alias("qty_cume_dist"),
        F.max("unit_price").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_max_unit_price"),
        F.lag("qty", 1, 0).over(w_all_by_unit_price).alias("prev_qty"),
        F.count(F.lit(1)).over(w_move_type_warehouse_id).alias("rows_in_move_type_warehouse_id"),
        F.nth_value("unit_price", 2).over(w_all_by_qty_full).alias("second_unit_price_2"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.sum("second_unit_price_2").over(w_all_by_move_type_warehouse_i).alias("second_unit_price_2_sum_2"),
        F.sum("seq_no").over(w_warehouse_id_by_rows_in_move).alias("running_seq_no"),
        F.max("second_unit_price").over(w_sku_id_move_type).alias("sku_id_move_type_max_second_unit_price"),
        F.dense_rank().over(w_all_by_move_type_warehouse_i_2).alias("move_type_warehouse_id_min_qty_dense_rank"),
        F.min("qty").over(w_sku_id_move_type).alias("sku_id_move_type_min_qty"),
        F.min("qty_cume_dist").over(w_sku_id_move_type).alias("sku_id_move_type_min_qty_cume_dist"),
        F.max("qty_cume_dist").over(w_sku_id_move_type).alias("sku_id_move_type_max_qty_cume_dist"),
    )
)

totals = (
    sequenced
    .filter(F.col("sku_id_move_type_max_second_unit_price") <= 4250.1)
    .groupBy("warehouse_id", "seq_no", "move_type_warehouse_id_min_qty_dense_rank")
    .agg(
        F.sum(F.when(F.col("operator").isNotNull(), F.col("rows_in_move_type_warehouse_id"))).alias("rows_in_move_type_warehouse_id_operator_is_not_n"),
        F.round(F.sum("second_unit_price"), 2).alias("total_second_unit_price"),
        F.count("*").alias("cnt"),
    )
)

windowed = (
    totals
    .select(
        "warehouse_id",
        "seq_no",
        "move_type_warehouse_id_min_qty_dense_rank",
        "rows_in_move_type_warehouse_id_operator_is_not_n",
        "total_second_unit_price",
        "cnt",
        F.round(F.col("seq_no") / F.nullif(F.sum("seq_no").over(w_warehouse_id), F.lit(0)), 4).alias("seq_no_share_of_warehouse_id"),
        F.last("total_second_unit_price", ignorenulls=True).over(w_all_by_rows_in_move_type_war).alias("last_known_total_second_unit_price"),
        F.round(F.col("total_second_unit_price") / F.nullif(F.sum("total_second_unit_price").over(w_warehouse_id), F.lit(0)), 4).alias("total_second_unit_price_share_of_warehouse_id"),
        F.round(F.avg("cnt").over(w_warehouse_id), 2).alias("warehouse_id_avg_cnt"),
    )
)

result = (
    windowed
    .select(
        "warehouse_id",
        "seq_no",
        "move_type_warehouse_id_min_qty_dense_rank",
        "rows_in_move_type_warehouse_id_operator_is_not_n",
        "total_second_unit_price",
        "cnt",
        "seq_no_share_of_warehouse_id",
        "last_known_total_second_unit_price",
        "total_second_unit_price_share_of_warehouse_id",
        "warehouse_id_avg_cnt",
        F.when(F.col("total_second_unit_price") >= 2550.2, "HIGH").when(F.col("total_second_unit_price") >= 750.8, "MEDIUM").otherwise("LOW").alias("total_second_unit_price_tier"),
        (F.col("total_second_unit_price") + F.col("seq_no")).alias("total_second_unit_price_plus_seq_no"),
    )
    .orderBy("seq_no", F.col("move_type_warehouse_id_min_qty_dense_rank").asc())
)
