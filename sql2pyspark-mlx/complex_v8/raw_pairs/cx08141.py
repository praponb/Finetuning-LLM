from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_warehouse_id_by_war = Window.partitionBy("operator", "warehouse_id").orderBy("warehouse_opened_on", "id")
w_warehouse_opened_on_year_ope = Window.partitionBy(F.year("warehouse_opened_on"), "operator").orderBy("warehouse_opened_on", "id")
w_warehouse_opened_on_year_ope_2 = Window.partitionBy(F.year("warehouse_opened_on"), "operator").orderBy("warehouse_opened_on")
w_operator_by_moved_at_full = Window.partitionBy("operator").orderBy(F.col("moved_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_moved_at_prior_2 = Window.partitionBy("operator").orderBy(F.col("moved_at").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_sku_id = Window.partitionBy("sku_id")
w_warehouse_opened_on_month_by = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("moved_at").desc(), "id").rowsBetween(-2, -1)
w_all_by_qty_prior_2 = Window.orderBy("qty", "id").rowsBetween(-2, -1)
w_warehouse_opened_on_month = Window.partitionBy(F.month("warehouse_opened_on"))
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_moved_at_month_operator = Window.partitionBy(F.month("moved_at"), "operator")
w_warehouse_opened_on_month_by_2 = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("moved_at").desc(), "id").rowsBetween(-6, -1)

valid_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

combined_details = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") > 2614), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.row_number().over(w_operator_warehouse_id_by_war).alias("warehouse_opened_on_seq"),
        F.lag("warehouse_opened_on").over(w_warehouse_opened_on_year_ope).alias("prev_warehouse_opened_on"),
        F.lag("moved_at").over(w_warehouse_opened_on_year_ope).alias("prev_moved_at"),
        F.dense_rank().over(w_warehouse_opened_on_year_ope_2).alias("warehouse_opened_on_dense_rank"),
        F.nth_value("moved_at", 2).over(w_operator_by_moved_at_full).alias("second_moved_at"),
        F.sum("qty").over(w_operator_by_moved_at_prior_2).alias("qty_sum_2_prior"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
        F.sum("unit_price").over(w_warehouse_opened_on_month_by).alias("unit_price_sum_2_prior"),
        F.round(F.avg("qty").over(w_all_by_qty_prior_2), 2).alias("qty_avg_2_prior"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_opened_on_month), F.lit(0)), 4).alias("qty_share_of_warehouse_opened_on_month"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.dense_rank().over(w_warehouse_id_by_moved_at).alias("moved_at_dense_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_moved_at_month_operator), 2).alias("unit_price_vs_moved_at_month_operator_avg"),
        F.round(F.avg("qty").over(w_warehouse_opened_on_month_by_2), 2).alias("qty_avg_6_prior"),
        F.count(F.lit(1)).over(w_warehouse_opened_on_month).alias("rows_in_warehouse_opened_on_month"),
        F.round(F.percent_rank().over(w_operator_warehouse_id_by_war), 3).alias("warehouse_opened_on_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "qty",
        "warehouse_opened_on_seq",
        "prev_warehouse_opened_on",
        "qty_sum_2_prior",
        "unit_price_sum_2_prior",
        "sku_id_avg_qty",
        "unit_price_vs_moved_at_month_operator_avg",
        "qty_avg_6_prior",
        "rows_in_warehouse_opened_on_month",
        "warehouse_opened_on_percent_rank",
    )
    .orderBy(
        F.col("rows_in_warehouse_opened_on_month").asc_nulls_last(),
        F.col("unit_price_vs_moved_at_month_operator_avg").asc_nulls_last(),
    )
)
