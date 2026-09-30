from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_ties = Window.orderBy("qty")
w_all_by_qty = Window.orderBy("qty", "id")
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_sku_id_operator_by_moved_at = Window.partitionBy("sku_id", "operator").orderBy("moved_at", "id").rowsBetween(-3, Window.currentRow)
w_sku_id_moved_at_to_date = Window.partitionBy("sku_id", F.to_date("moved_at"))
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_sku_id_operator_by_moved_at_2 = Window.partitionBy("sku_id", "operator").orderBy("moved_at", "id")
w_all_by_qty_running = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_next_moved_at_month_b = Window.partitionBy("sku_id", F.date_trunc("month", "next_moved_at")).orderBy("warehouse_id_operator_sum_qty", "id")
w_all_by_moved_at_full_2 = Window.orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_qty_rank = Window.partitionBy("move_type", "qty_rank")
w_all_by_moved_at_2 = Window.orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_all_by_qty_percent_rank_ties = Window.orderBy(F.col("qty_percent_rank").desc())

valid_moves = (
    df_stock_moves
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws("|", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

with_details = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.lower(F.col("w.wh_code")).like("%ku%")), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.operator_move_type_label",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

with_windows = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "operator_move_type_label",
        "warehouse_wh_code",
        "warehouse_capacity",
        F.rank().over(w_all_by_qty_ties).alias("qty_rank"),
        F.round(F.percent_rank().over(w_all_by_qty), 3).alias("qty_percent_rank"),
        F.nth_value("qty", 2).over(w_all_by_moved_at_full).alias("second_qty"),
        (F.col("qty") - F.lag("qty").over(w_all_by_moved_at)).alias("qty_change"),
        F.count(F.lit(1)).over(w_sku_id_warehouse_id).alias("rows_in_sku_id_warehouse_id"),
        F.round(F.avg("unit_price").over(w_sku_id_operator_by_moved_at), 2).alias("unit_price_avg_4"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_sku_id_moved_at_to_date), 2).alias("warehouse_capacity_vs_sku_id_moved_at_to_date_av"),
        F.sum("qty").over(w_warehouse_id_operator).alias("warehouse_id_operator_sum_qty"),
        F.first("qty").over(w_all_by_moved_at_full).alias("first_qty"),
        F.round(F.avg("unit_price").over(w_warehouse_id_operator), 2).alias("warehouse_id_operator_avg_unit_price"),
        F.lead("moved_at").over(w_sku_id_operator_by_moved_at_2).alias("next_moved_at"),
        F.max("qty").over(w_all_by_qty_running).alias("max_qty_so_far"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.lag("second_qty", 3).over(w_sku_id_next_moved_at_month_b).alias("prev3_second_qty"),
        F.nth_value("warehouse_capacity", 2).over(w_all_by_moved_at_full_2).alias("second_warehouse_capacity"),
        F.round(F.avg("qty_rank").over(w_move_type_qty_rank), 2).alias("move_type_qty_rank_avg_qty_rank"),
        F.lag("max_qty_so_far", 3).over(w_all_by_moved_at_2).alias("prev3_max_qty_so_far"),
        F.sum("qty").over(w_move_type_qty_rank).alias("move_type_qty_rank_sum_qty"),
        F.size(F.collect_set("sku_id").over(w_move_type_qty_rank)).alias("distinct_sku_id_in_move_type_qty_rank"),
        F.round(F.percent_rank().over(w_all_by_qty_percent_rank_ties), 3).alias("qty_percent_rank_percent_rank"),
    )
)

grouped = (
    with_trends
    .groupBy(
        F.date_trunc("month", "next_moved_at").alias("next_moved_at_month"),
        "move_type",
    )
    .agg(
        F.sum(F.when(F.lower(F.col("warehouse_wh_code")).like("%ku%"), 1).otherwise(0)).alias("n_lower_warehouse_wh_code_like"),
        F.percentile_approx("qty", 0.5).alias("median_qty"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.count(F.when(F.upper(F.trim(F.col("warehouse_wh_code"))).like("B%"), 1)).alias("n_upper_trim_warehouse_wh_code"),
        F.percentile_approx("warehouse_capacity", 0.5).alias("median_warehouse_capacity"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.max("move_type_qty_rank_avg_qty_rank").alias("max_move_type_qty_rank_avg_qty_rank_check"),
    )
    .filter(F.col("max_move_type_qty_rank_avg_qty_rank_check") > 3.2)
    .drop("max_move_type_qty_rank_avg_qty_rank_check")
)

result = (
    grouped
    .filter(F.col("next_moved_at_month") >= F.lit("2025-01-17 09:00:00").cast("timestamp"))
    .select(
        "next_moved_at_month",
        "move_type",
        "n_lower_warehouse_wh_code_like",
        "median_qty",
        "distinct_warehouse_ids",
        "median_warehouse_capacity",
        "move_type_list",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
    .orderBy(F.col("median_warehouse_capacity").asc())
)
