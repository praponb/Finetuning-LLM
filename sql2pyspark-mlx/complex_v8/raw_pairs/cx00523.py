from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_unit_price_to_qt = Window.partitionBy("operator").orderBy(F.col("unit_price_to_qty_ratio").desc(), "operator", "move_type", "moved_at_month").rowsBetween(-3, Window.currentRow)
w_move_type_by_first_moved_at = Window.partitionBy("move_type").orderBy(F.col("first_moved_at").desc(), "operator", "move_type", "moved_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_unit_price_to_qt_2 = Window.partitionBy("operator").orderBy(F.col("unit_price_to_qty_ratio").desc(), "operator", "move_type", "moved_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_unit_price_to_qt_3 = Window.partitionBy("operator").orderBy(F.col("unit_price_to_qty_ratio").desc(), "operator", "move_type", "moved_at_month")
w_operator_by_first_moved_at = Window.partitionBy("operator").orderBy(F.col("first_moved_at").desc_nulls_first(), "operator", "move_type", "moved_at_month")
w_move_type_by_first_moved_at_2 = Window.partitionBy("move_type").orderBy(F.col("first_moved_at").desc(), "operator", "move_type", "moved_at_month")
w_unit_price_to_qty_ratio_rank = Window.partitionBy("unit_price_to_qty_ratio_rank").orderBy("first_total_unit_price_bucket", "operator", "move_type", "moved_at_month")

prepared_moves = (
    df_stock_moves
    .filter(F.col("operator") == "op_eve")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("operator_ref"),
    )
)

per_group = (
    prepared_moves
    .filter(F.col("qty").between(67, 314))
    .groupBy(
        "operator",
        "move_type",
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
    )
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.sum("unit_price").alias("total_unit_price"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.round(F.sum("unit_price") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("unit_price_to_qty_ratio"),
        F.max("qty").alias("max_qty_check"),
    )
    .filter(F.col("max_qty_check") > 123)
    .drop("max_qty_check")
)

scored = (
    per_group
    .select(
        "operator",
        "move_type",
        "moved_at_month",
        "first_moved_at",
        "move_type_list",
        "avg_unit_price",
        "avg_qty",
        "unit_price_to_qty_ratio",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_moved_at")).alias("days_since_first_moved_at"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        (F.floor(F.col("total_unit_price") / 500) * 500).alias("total_unit_price_bucket"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.sum("days_since_first_moved_at").over(w_operator_by_unit_price_to_qt).alias("days_since_first_moved_at_sum_4"),
        F.first("total_unit_price_bucket").over(w_move_type_by_first_moved_at).alias("first_total_unit_price_bucket"),
        F.nth_value("days_since_first_moved_at", 2).over(w_operator_by_unit_price_to_qt_2).alias("second_days_since_first_moved_at"),
        F.rank().over(w_operator_by_unit_price_to_qt_3).alias("unit_price_to_qty_ratio_rank"),
        F.row_number().over(w_operator_by_first_moved_at).alias("first_moved_at_seq"),
        F.lead("avg_qty").over(w_move_type_by_first_moved_at_2).alias("next_avg_qty"),
    )
)

with_activity = (
    with_windows
    .crossJoin(with_windows.agg(F.avg("avg_unit_price").alias("overall_avg_avg_unit_price")))
    .filter(F.col("avg_unit_price") > F.col("overall_avg_avg_unit_price"))
    .drop("overall_avg_avg_unit_price")
)

with_trends = (
    with_activity
    .select(
        "operator",
        "move_type",
        "moved_at_month",
        "first_moved_at",
        "move_type_list",
        "avg_qty",
        "move_type_slug",
        "total_unit_price_bucket",
        "days_since_first_moved_at_sum_4",
        "second_days_since_first_moved_at",
        "unit_price_to_qty_ratio_rank",
        "first_moved_at_seq",
        "next_avg_qty",
        F.round(F.cume_dist().over(w_unit_price_to_qty_ratio_rank), 3).alias("first_total_unit_price_bucket_cume_dist"),
        F.dense_rank().over(w_unit_price_to_qty_ratio_rank).alias("first_total_unit_price_bucket_dense_rank"),
        F.row_number().over(w_unit_price_to_qty_ratio_rank).alias("first_total_unit_price_bucket_seq"),
    )
)

exploded = (
    with_trends
    .select(
        "operator",
        "move_type",
        "moved_at_month",
        "first_moved_at",
        "avg_qty",
        "move_type_slug",
        "total_unit_price_bucket",
        "days_since_first_moved_at_sum_4",
        "second_days_since_first_moved_at",
        "unit_price_to_qty_ratio_rank",
        "first_moved_at_seq",
        "next_avg_qty",
        "first_total_unit_price_bucket_cume_dist",
        "first_total_unit_price_bucket_dense_rank",
        "first_total_unit_price_bucket_seq",
        F.explode("move_type_list").alias("move_type_list_item"),
    )
)

result = (
    exploded
    .select(
        "operator",
        "moved_at_month",
        "first_moved_at",
        "total_unit_price_bucket",
        "days_since_first_moved_at_sum_4",
        "first_moved_at_seq",
        "next_avg_qty",
        "first_total_unit_price_bucket_cume_dist",
        "first_total_unit_price_bucket_dense_rank",
        "first_total_unit_price_bucket_seq",
        "move_type_list_item",
        F.upper(F.trim(F.col("move_type_list_item"))).alias("move_type_list_item_clean"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
    .orderBy(F.col("total_unit_price_bucket").asc_nulls_last(), "moved_at_month")
)
