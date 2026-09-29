from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_move_type = Window.partitionBy("operator", "move_type")
w_sku_id_by_qty_full = Window.partitionBy("sku_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id")
w_warehouse_id_by_qty_running = Window.partitionBy("warehouse_id").orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_qty_ties = Window.partitionBy("warehouse_id").orderBy(F.col("qty").asc_nulls_last())
w_warehouse_id_by_moved_at_ful = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_distinct_move_type_in_sku_id = Window.partitionBy("distinct_move_type_in_sku_id_operator", "min_distinct_move_type_in_sku_id_operator").orderBy("last_known_qty_operator_not_in_op_ann_op_de")
w_operator_by_min_distinct_mov = Window.partitionBy("operator").orderBy("min_distinct_move_type_in_sku_id_operator", "distinct_move_type_in_sku_id_operator", "second_moved_at_month", "operator").rowsBetween(-3, Window.currentRow)
w_operator_by_min_distinct_mov_2 = Window.partitionBy("operator").orderBy("min_distinct_move_type_in_sku_id_operator", "distinct_move_type_in_sku_id_operator", "second_moved_at_month", "operator").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_row_count_distinct_move_type = Window.partitionBy("row_count", "distinct_move_type_in_sku_id_operator")

filtered = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")) & (F.col("qty") > 98))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator_move_type), F.lit(0)), 4).alias("qty_share_of_operator_move_type"),
        F.nth_value("moved_at", 2).over(w_sku_id_by_qty_full).alias("second_moved_at"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_sku_id_by_moved_at))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.last("qty", ignorenulls=True).over(w_warehouse_id_by_qty_running).alias("last_known_qty"),
        F.round(F.percent_rank().over(w_warehouse_id_by_qty_ties), 3).alias("qty_percent_rank"),
        F.first("qty_filled").over(w_warehouse_id_by_moved_at_ful).alias("first_qty_filled"),
        F.row_number().over(w_warehouse_id_by_moved_at).alias("moved_at_seq"),
        F.size(F.collect_set("move_type").over(w_sku_id_operator)).alias("distinct_move_type_in_sku_id_operator"),
        F.max("qty").over(w_operator_move_type).alias("operator_move_type_max_qty"),
        F.round(F.col("qty_filled") / F.nullif(F.sum("qty_filled").over(w_operator_move_type), F.lit(0)), 4).alias("qty_filled_share_of_operator_move_type"),
    )
)

leaders = (
    windowed
    .filter(F.col("moved_at_seq") == 1)
    .drop("moved_at_seq")
)

rollup_base = (
    leaders
    .filter(F.col("operator") == "op_bee")
    .groupBy(
        "distinct_move_type_in_sku_id_operator",
        F.date_trunc("month", "second_moved_at").alias("second_moved_at_month"),
        "operator",
    )
    .agg(
        F.sum("qty_share_of_operator_move_type").alias("total_qty_share_of_operator_move_type"),
        F.min("distinct_move_type_in_sku_id_operator").alias("min_distinct_move_type_in_sku_id_operator"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.round(F.stddev("qty"), 2).alias("stddev_qty"),
        F.sum(F.when(~F.col("operator").isin("op_ann", "op_dew"), F.col("last_known_qty"))).alias("last_known_qty_operator_not_in_op_ann_op_de"),
        F.count("*").alias("row_count"),
    )
)

with_trends = (
    rollup_base
    .select(
        "distinct_move_type_in_sku_id_operator",
        "second_moved_at_month",
        "operator",
        "total_qty_share_of_operator_move_type",
        "min_distinct_move_type_in_sku_id_operator",
        "stddev_qty",
        "last_known_qty_operator_not_in_op_ann_op_de",
        F.dense_rank().over(w_distinct_move_type_in_sku_id).alias("last_known_qty_operator_not_in_op_ann_op_de_dens"),
        F.round(F.avg("row_count").over(w_operator_by_min_distinct_mov), 2).alias("row_count_avg_4"),
        F.nth_value("row_count", 2).over(w_operator_by_min_distinct_mov_2).alias("second_row_count"),
        F.size(F.collect_set("operator").over(w_row_count_distinct_move_type)).alias("distinct_operator_in_row_count_distinct_move_typ"),
    )
)

result = (
    with_trends
    .select(
        "distinct_move_type_in_sku_id_operator",
        "operator",
        "total_qty_share_of_operator_move_type",
        "min_distinct_move_type_in_sku_id_operator",
        "stddev_qty",
        "last_known_qty_operator_not_in_op_ann_op_de",
        "row_count_avg_4",
        "distinct_operator_in_row_count_distinct_move_typ",
        (F.col("row_count_avg_4") + F.col("stddev_qty")).alias("row_count_avg_4_plus_stddev_qty"),
    )
    .orderBy(
        F.col("row_count_avg_4_plus_stddev_qty").desc(),
        "distinct_operator_in_row_count_distinct_move_typ",
    )
)
