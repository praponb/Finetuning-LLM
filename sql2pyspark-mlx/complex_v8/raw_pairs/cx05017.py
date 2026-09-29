from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_operator_by_moved_at = Window.partitionBy("sku_id", "operator").orderBy(F.col("moved_at").desc())
w_warehouse_opened_on_year_by = Window.partitionBy(F.year("warehouse_opened_on")).orderBy("qty")
w_warehouse_id_operator_by_mov = Window.partitionBy("warehouse_id", "operator").orderBy("moved_at", "id")
w_warehouse_opened_on_year_by_2 = Window.partitionBy(F.year("warehouse_opened_on")).orderBy("qty", "id").rowsBetween(-6, -1)
w_warehouse_id_moved_at_month = Window.partitionBy("warehouse_id", F.date_trunc("month", "moved_at"))
w_warehouse_opened_on_year_by_3 = Window.partitionBy(F.year("warehouse_opened_on")).orderBy("qty", "id")
w_warehouse_id_operator_by_mov_2 = Window.partitionBy("warehouse_id", "operator").orderBy("moved_at", "id").rowsBetween(-2, -1)
w_sku_id_operator_by_moved_at_2 = Window.partitionBy("sku_id", "operator").orderBy(F.col("moved_at").desc(), "id")
w_warehouse_id_operator_by_mov_3 = Window.partitionBy("warehouse_id", "operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_operator_by_mov_4 = Window.partitionBy("warehouse_id", "operator").orderBy("moved_at")
w_warehouse_opened_on_month_di = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on"), "distinct_sku_id_in_warehouse_id_moved_at_month").orderBy("moved_at_seq")
w_operator = Window.partitionBy("operator")
w_all_by_moved_at_rank = Window.orderBy(F.col("moved_at_rank").asc_nulls_last(), "id")
w_all_by_moved_at_rank_full = Window.orderBy(F.col("moved_at_rank").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_rank_moving = Window.orderBy(F.col("moved_at_rank").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_warehouse_opened_on_month_di_2 = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on"), "distinct_sku_id_in_warehouse_id_moved_at_month").orderBy("moved_at_seq", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

combined_details_warehouse = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.move_type_slug",
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

metrics = (
    combined_details_warehouse
    .select(
        "id",
        "sku_id",
        "moved_at",
        "operator",
        "move_type_slug",
        "warehouse_opened_on",
        F.round(F.cume_dist().over(w_sku_id_operator_by_moved_at), 3).alias("moved_at_cume_dist"),
        F.round(F.cume_dist().over(w_warehouse_opened_on_year_by), 3).alias("qty_cume_dist"),
        F.row_number().over(w_warehouse_id_operator_by_mov).alias("rn"),
        F.round(F.avg("qty").over(w_warehouse_opened_on_year_by_2), 2).alias("qty_avg_6_prior"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id_moved_at_month)).alias("distinct_sku_id_in_warehouse_id_moved_at_month"),
        F.ntile(3).over(w_warehouse_opened_on_year_by_3).alias("qty_tercile"),
        F.lag("warehouse_opened_on").over(w_warehouse_opened_on_year_by_3).alias("prev_warehouse_opened_on"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id_moved_at_month), 2).alias("qty_vs_warehouse_id_moved_at_month_avg"),
        F.sum("qty").over(w_warehouse_id_operator_by_mov_2).alias("qty_sum_2_prior"),
        F.min("qty").over(w_warehouse_id_moved_at_month).alias("warehouse_id_moved_at_month_min_qty"),
        F.rank().over(w_sku_id_operator_by_moved_at_2).alias("moved_at_rank"),
        F.count(F.lit(1)).over(w_warehouse_id_operator_by_mov_3).alias("running_count"),
        F.row_number().over(w_sku_id_operator_by_moved_at_2).alias("moved_at_seq"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id_moved_at_month), F.lit(0)), 4).alias("qty_share_of_warehouse_id_moved_at_month"),
        F.round(F.cume_dist().over(w_warehouse_id_operator_by_mov_4), 3).alias("moved_at_cume_dist_2"),
    )
)

annotated = (
    metrics
    .select(
        "*",
        F.substring(F.trim(F.col("move_type_slug")), 1, 4).alias("move_type_slug_prefix"),
        F.concat(F.col("move_type_slug"), F.lit("#"), F.col("qty_sum_2_prior").cast("string")).alias("move_type_slug_ref"),
        F.round(F.col("qty_tercile") / F.nullif(F.col("moved_at_cume_dist"), F.lit(0)), 3).alias("qty_tercile_per_moved_at_cume_dist"),
    )
)

with_windows = (
    annotated
    .select(
        "id",
        "moved_at",
        "operator",
        "move_type_slug",
        "distinct_sku_id_in_warehouse_id_moved_at_month",
        "qty_tercile",
        "prev_warehouse_opened_on",
        "qty_vs_warehouse_id_moved_at_month_avg",
        "qty_sum_2_prior",
        "warehouse_id_moved_at_month_min_qty",
        "moved_at_rank",
        "running_count",
        "moved_at_cume_dist_2",
        "move_type_slug_prefix",
        "qty_tercile_per_moved_at_cume_dist",
        F.round(F.percent_rank().over(w_warehouse_opened_on_month_di), 3).alias("moved_at_seq_percent_rank"),
        F.round(F.avg("running_count").over(w_operator), 2).alias("operator_avg_running_count"),
        F.lead("moved_at_seq").over(w_all_by_moved_at_rank).alias("next_moved_at_seq"),
        F.nth_value("moved_at_rank", 2).over(w_all_by_moved_at_rank_full).alias("second_moved_at_rank"),
        F.max("rn").over(w_all_by_moved_at_rank_moving).alias("rn_max_2"),
        F.nth_value("prev_warehouse_opened_on", 2).over(w_warehouse_opened_on_month_di_2).alias("second_prev_warehouse_opened_on"),
        F.nth_value("rn", 2).over(w_warehouse_opened_on_month_di_2).alias("second_rn"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "moved_at",
        "operator",
        "move_type_slug",
        "qty_vs_warehouse_id_moved_at_month_avg",
        "qty_sum_2_prior",
        "moved_at_rank",
        "moved_at_cume_dist_2",
        "move_type_slug_prefix",
        "qty_tercile_per_moved_at_cume_dist",
        "operator_avg_running_count",
        "next_moved_at_seq",
        "second_moved_at_rank",
        "rn_max_2",
        "second_prev_warehouse_opened_on",
        "second_rn",
        F.round(F.col("second_rn") / F.nullif(F.col("moved_at_cume_dist_2"), F.lit(0)), 3).alias("second_rn_per_moved_at_cume_dist_2"),
    )
)
