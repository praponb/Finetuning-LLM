from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_all_by_qty_full = Window.orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id = Window.partitionBy("sku_id")
w_sku_id_by_moved_at_full = Window.partitionBy("sku_id").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator = Window.partitionBy("operator")
w_operator_by_moved_at_full = Window.partitionBy("operator").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_first_unit_pric = Window.partitionBy("move_type").orderBy(F.col("first_unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_day_month = Window.partitionBy(F.date_trunc("month", "moved_at_day"))
w_sku_id_avg_qty_tier_by_n_ope = Window.partitionBy("sku_id_avg_qty_tier").orderBy("n_operator_not_in_op_ann_op_be", "sku_id_avg_qty_tier", "moved_at_day", "distinct_move_type_in_operator").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.round(F.percent_rank().over(w_sku_id_by_moved_at), 3).alias("moved_at_percent_rank"),
        F.nth_value("unit_price", 2).over(w_all_by_qty_full).alias("second_unit_price"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.round(F.col("days_since_moved_at") / F.nullif(F.sum("days_since_moved_at").over(w_sku_id), F.lit(0)), 4).alias("days_since_moved_at_share_of_sku_id"),
        F.nth_value("days_since_moved_at", 2).over(w_all_by_qty_full).alias("second_days_since_moved_at"),
        F.nth_value("moved_at", 2).over(w_sku_id_by_moved_at_full).alias("second_moved_at"),
        F.nth_value("unit_price", 2).over(w_sku_id_by_moved_at_full).alias("second_unit_price_2"),
        F.first("unit_price").over(w_all_by_qty_full).alias("first_unit_price"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "sku_id",
        "moved_at",
        "operator",
        "days_since_moved_at",
        "moved_at_percent_rank",
        "second_unit_price",
        "sku_id_avg_qty",
        "days_since_moved_at_share_of_sku_id",
        "second_days_since_moved_at",
        "second_moved_at",
        "second_unit_price_2",
        "first_unit_price",
        F.min("unit_price").over(w_operator).alias("operator_min_unit_price"),
        F.min("days_since_moved_at").over(w_operator).alias("operator_min_days_since_moved_at"),
        F.max("second_days_since_moved_at").over(w_operator).alias("operator_max_second_days_since_moved_at"),
        F.last("unit_price").over(w_operator_by_moved_at_full).alias("last_unit_price"),
        F.size(F.collect_set("move_type").over(w_operator)).alias("distinct_move_type_in_operator"),
        F.last("sku_id_avg_qty", ignorenulls=True).over(w_move_type_by_first_unit_pric).alias("last_known_sku_id_avg_qty"),
    )
)

grouped = (
    with_windows
    .groupBy(
        F.when(F.col("sku_id_avg_qty") >= 350.3, "large").when(F.col("sku_id_avg_qty") >= 150.7, "medium").otherwise("small").alias("sku_id_avg_qty_tier"),
        F.to_date("moved_at").alias("moved_at_day"),
        "distinct_move_type_in_operator",
    )
    .agg(
        F.round(F.avg("first_unit_price"), 2).alias("avg_first_unit_price"),
        F.min("sku_id_avg_qty").alias("min_sku_id_avg_qty"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.round(F.sum("moved_at_percent_rank"), 2).alias("total_moved_at_percent_rank"),
        F.sum(F.when(~F.col("operator").isin("op_ann", "op_bee"), 1).otherwise(0)).alias("n_operator_not_in_op_ann_op_be"),
        F.sum("operator_min_unit_price").alias("total_operator_min_unit_price"),
    )
)

with_trends_2 = (
    grouped
    .select(
        "*",
        F.count(F.lit(1)).over(w_moved_at_day_month).alias("rows_in_moved_at_day_month"),
        F.size(F.collect_set("sku_id_avg_qty_tier").over(w_moved_at_day_month)).alias("distinct_sku_id_avg_qty_tier_in_moved_at_day_mon"),
        F.last("min_sku_id_avg_qty", ignorenulls=True).over(w_sku_id_avg_qty_tier_by_n_ope).alias("last_known_min_sku_id_avg_qty"),
    )
)

result = (
    with_trends_2
    .select(
        "sku_id_avg_qty_tier",
        "moved_at_day",
        "distinct_move_type_in_operator",
        "avg_first_unit_price",
        "min_sku_id_avg_qty",
        "operator_list",
        "n_operator_not_in_op_ann_op_be",
        "distinct_sku_id_avg_qty_tier_in_moved_at_day_mon",
        "last_known_min_sku_id_avg_qty",
        F.abs(F.col("min_sku_id_avg_qty") - 325.4).alias("min_sku_id_avg_qty_distance"),
        F.round(F.col("avg_first_unit_price") * 0.25, 2).alias("avg_first_unit_price_adj"),
    )
    .orderBy(
        F.col("distinct_move_type_in_operator").desc(),
        F.col("avg_first_unit_price").asc(),
    )
)
