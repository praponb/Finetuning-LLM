from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_warehouse_opened_on = Window.orderBy("warehouse_opened_on", "id")
w_sku_id = Window.partitionBy("sku_id")
w_all_by_qty_running = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_warehouse_id_by_wa = Window.partitionBy("move_type", "warehouse_id").orderBy("warehouse_opened_on", "id")
w_all_by_warehouse_opened_on_r = Window.orderBy("warehouse_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_month = Window.partitionBy(F.date_trunc("month", "moved_at"))
w_warehouse_opened_on_month_by = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy("moved_at")
w_all_by_qty_ties = Window.orderBy("qty")
w_all_by_warehouse_opened_on_t = Window.orderBy("warehouse_opened_on")
w_move_type_warehouse_id_by_wa_2 = Window.partitionBy("move_type", "warehouse_id").orderBy("warehouse_opened_on", "id").rowsBetween(-3, -1)
w_all_by_qty_full = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_warehouse_id_by_wa_3 = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(-2, -1)
w_all_by_qty_share_of_moved_at = Window.orderBy(F.col("qty_share_of_moved_at_month").desc_nulls_first(), "id")
w_all_by_qty_share_of_moved_at_2 = Window.orderBy("qty_share_of_moved_at_month")
w_all_by_qty_share_of_moved_at_3 = Window.orderBy(F.col("qty_share_of_moved_at_month").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_warehouse_opened_on_month = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on"))
w_first_warehouse_opened_on_mo = Window.partitionBy(F.date_trunc("month", "first_warehouse_opened_on"), "moved_at_rank").orderBy("first_warehouse_opened_on", "id")
w_warehouse_opened_on_year_by = Window.partitionBy(F.year("warehouse_opened_on")).orderBy("sku_id_avg_qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("unit_price") > 152.7))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

combined_details = (
    scoped_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

with_trends = (
    combined_details
    .select(
        "*",
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_all_by_warehouse_opened_on))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.max("qty").over(w_all_by_qty_running).alias("max_qty_so_far"),
        F.ntile(10).over(w_move_type_warehouse_id_by_wa).alias("warehouse_opened_on_decile"),
        F.max("qty").over(w_all_by_warehouse_opened_on_r).alias("max_qty_so_far_2"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_moved_at_month), F.lit(0)), 4).alias("qty_share_of_moved_at_month"),
        F.round(F.avg("qty").over(w_moved_at_month), 2).alias("moved_at_month_avg_qty"),
        F.rank().over(w_warehouse_opened_on_month_by).alias("moved_at_rank"),
        F.round(F.cume_dist().over(w_all_by_qty_ties), 3).alias("qty_cume_dist"),
        F.rank().over(w_all_by_warehouse_opened_on_t).alias("warehouse_opened_on_rank"),
        F.sum("qty").over(w_move_type_warehouse_id_by_wa_2).alias("qty_sum_3_prior"),
        F.first("warehouse_opened_on").over(w_all_by_qty_full).alias("first_warehouse_opened_on"),
        F.max("qty").over(w_move_type_warehouse_id_by_wa_3).alias("qty_max_2_prior"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        (F.col("qty_sum_3_prior") - F.lag("qty_sum_3_prior").over(w_all_by_qty_share_of_moved_at)).alias("qty_sum_3_prior_change"),
        F.round(F.cume_dist().over(w_all_by_qty_share_of_moved_at_2), 3).alias("qty_share_of_moved_at_month_cume_dist"),
        F.max("qty_sum_3_prior").over(w_all_by_qty_share_of_moved_at_3).alias("qty_sum_3_prior_max_2_prior"),
        F.count(F.lit(1)).over(w_warehouse_opened_on_month).alias("rows_in_warehouse_opened_on_month"),
        (F.col("warehouse_opened_on_decile") - F.lag("warehouse_opened_on_decile").over(w_first_warehouse_opened_on_mo)).alias("warehouse_opened_on_decile_change"),
        F.first("sku_id_avg_qty").over(w_warehouse_opened_on_year_by).alias("first_sku_id_avg_qty"),
    )
)

totals = (
    with_windows
    .groupBy(
        "sku_id",
        F.when(F.col("first_sku_id_avg_qty") >= 300.4, "large").when(F.col("first_sku_id_avg_qty") >= 200.6, "medium").otherwise("small").alias("first_sku_id_avg_qty_tier"),
        F.date_format("moved_at", "yyyy-MM").alias("moved_at_ym"),
    )
    .agg(
        F.datediff(F.max("warehouse_opened_on"), F.min("warehouse_opened_on")).alias("warehouse_opened_on_span_days"),
        F.round(F.sum("qty_sum_3_prior_change") / F.nullif(F.sum("qty_sum_3_prior_max_2_prior"), F.lit(0)), 3).alias("qty_sum_3_prior_change_to_qty_sum_3_prior_max_2_"),
    )
)

result = (
    totals
    .select(
        "sku_id",
        "first_sku_id_avg_qty_tier",
        "moved_at_ym",
        "warehouse_opened_on_span_days",
        "qty_sum_3_prior_change_to_qty_sum_3_prior_max_2_",
        F.substring(F.trim(F.col("moved_at_ym")), 1, 2).alias("moved_at_ym_prefix"),
        F.substring(F.trim(F.col("moved_at_ym")), 1, 2).alias("moved_at_ym_prefix_2"),
    )
)
