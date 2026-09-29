from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_moved_at_ful = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty_full = Window.orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_sku_id = Window.partitionBy("sku_id")
w_all_by_qty = Window.orderBy(F.col("qty").asc_nulls_last(), "id")
w_warehouse_id_by_moved_at_tie = Window.partitionBy("warehouse_id").orderBy("moved_at")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_move_type_by_qty = Window.partitionBy("move_type").orderBy(F.col("qty").asc_nulls_last(), "id")
w_all_by_qty_ties = Window.orderBy(F.col("qty").asc_nulls_last())
w_move_type_or_unknown_by_qty = Window.partitionBy("move_type_or_unknown").orderBy(F.col("qty_rank").desc(), "id")
w_all_by_qty_change = Window.orderBy("qty_change", "id")
w_move_type_or_unknown_by_qty_2 = Window.partitionBy("move_type_or_unknown").orderBy(F.col("qty_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty_change_ties = Window.orderBy("qty_change")
w_second_moved_at_month = Window.partitionBy(F.date_trunc("month", "second_moved_at"))
w_qty_rank = Window.partitionBy("qty_rank")
w_all_by_qty_change_running = Window.orderBy("qty_change", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

with_parent_sku = (
    recent_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "sk.sku_code",
    )
)

with_trends = (
    with_parent_sku
    .select(
        "*",
        F.nth_value("moved_at", 2).over(w_warehouse_id_by_moved_at_ful).alias("second_moved_at"),
        F.first("moved_at").over(w_all_by_qty_full).alias("first_moved_at"),
        F.nth_value("qty", 2).over(w_all_by_moved_at_full).alias("second_qty"),
        F.dense_rank().over(w_all_by_moved_at).alias("moved_at_dense_rank"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        (F.col("qty") - F.lag("qty").over(w_all_by_qty)).alias("qty_change"),
        F.row_number().over(w_all_by_moved_at).alias("rn"),
        F.round(F.percent_rank().over(w_warehouse_id_by_moved_at_tie), 3).alias("moved_at_percent_rank"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id), F.lit(0)), 4).alias("qty_share_of_warehouse_id"),
        F.rank().over(w_move_type_by_qty).alias("qty_rank"),
        F.round(F.percent_rank().over(w_all_by_qty_ties), 3).alias("qty_percent_rank"),
        F.size(F.collect_set("move_type").over(w_warehouse_id)).alias("distinct_move_type_in_warehouse_id"),
        F.sum("qty").over(w_sku_id).alias("sku_id_sum_qty"),
    )
)

scored = (
    with_trends
    .filter(F.col("moved_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp"))
    .withColumn("move_type_or_unknown", F.coalesce(F.col("move_type"), F.lit("UNKNOWN")))
)

sequenced = (
    scored
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "first_moved_at",
        "second_qty",
        "moved_at_dense_rank",
        "rows_in_sku_id",
        "rn",
        "sku_id_avg_qty",
        "qty_share_of_warehouse_id",
        "qty_rank",
        "qty_percent_rank",
        "distinct_move_type_in_warehouse_id",
        "sku_id_sum_qty",
        "move_type_or_unknown",
        F.lead("rows_in_sku_id", 3).over(w_move_type_or_unknown_by_qty).alias("next3_rows_in_sku_id"),
        (F.col("second_qty") - F.lag("second_qty").over(w_all_by_qty_change)).alias("second_qty_change"),
        F.nth_value("qty_change", 2).over(w_move_type_or_unknown_by_qty_2).alias("second_qty_change_2"),
        F.round(F.cume_dist().over(w_all_by_qty_change_ties), 3).alias("qty_change_cume_dist"),
        F.size(F.collect_set("warehouse_id").over(w_second_moved_at_month)).alias("distinct_warehouse_id_in_second_moved_at_month"),
        F.count(F.lit(1)).over(w_qty_rank).alias("rows_in_qty_rank"),
        F.sum("qty_share_of_warehouse_id").over(w_all_by_qty_change_running).alias("running_qty_share_of_warehouse_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "first_moved_at",
        "second_qty",
        "moved_at_dense_rank",
        "rows_in_sku_id",
        "rn",
        "sku_id_avg_qty",
        "qty_share_of_warehouse_id",
        "qty_rank",
        "qty_percent_rank",
        "sku_id_sum_qty",
        "move_type_or_unknown",
        "next3_rows_in_sku_id",
        "second_qty_change",
        "second_qty_change_2",
        "running_qty_share_of_warehouse_id",
        F.when(F.col("qty_percent_rank") >= 0.8, "A").when(F.col("qty_percent_rank") >= 0.4, "B").otherwise("C").alias("qty_percent_rank_tier"),
    )
)
