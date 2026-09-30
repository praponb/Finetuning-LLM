from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_year_operator = Window.partitionBy(F.year("moved_at"), "operator")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_all_by_qty = Window.orderBy("qty", "id")
w_all_by_unit_price_filled_tie = Window.orderBy("unit_price_filled")
w_all_by_qty_full = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty_prior_6 = Window.orderBy("qty", "id").rowsBetween(-6, -1)
w_operator = Window.partitionBy("operator")
w_all_by_unit_price_filled = Window.orderBy("unit_price_filled", "id")
w_all_by_unit_price_prior_2 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(-2, -1)
w_operator_by_unit_price_full = Window.partitionBy("operator").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_unit_price = Window.partitionBy("operator").orderBy(F.col("unit_price").desc(), "id")
w_all_by_unit_price_full = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_to_date = Window.partitionBy(F.to_date("moved_at"))
w_warehouse_id_by_unit_price_s = Window.partitionBy("warehouse_id").orderBy("unit_price_share_of_operator", "id")

scoped = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(10, 13)) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.count(F.lit(1)).over(w_moved_at_year_operator).alias("rows_in_moved_at_year_operator"),
        F.lead("moved_at").over(w_all_by_unit_price).alias("next_moved_at"),
        F.ntile(3).over(w_all_by_qty).alias("qty_tercile"),
        F.dense_rank().over(w_all_by_unit_price_filled_tie).alias("unit_price_filled_dense_rank"),
        F.last("qty").over(w_all_by_qty_full).alias("last_qty"),
        F.round(F.avg("qty").over(w_all_by_qty_prior_6), 2).alias("qty_avg_6_prior"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator), F.lit(0)), 4).alias("unit_price_share_of_operator"),
        F.lag("unit_price_filled").over(w_all_by_unit_price_filled).alias("prev_unit_price_filled"),
        F.max("unit_price").over(w_all_by_unit_price_prior_2).alias("unit_price_max_2_prior"),
        F.first("qty").over(w_operator_by_unit_price_full).alias("first_qty"),
        F.lead("unit_price_filled", 3, 0).over(w_all_by_unit_price).alias("next3_unit_price_filled"),
        F.round(F.col("qty") - F.avg("qty").over(w_operator), 2).alias("qty_vs_operator_avg"),
        F.row_number().over(w_operator_by_unit_price).alias("seq_no"),
        F.nth_value("qty", 2).over(w_all_by_unit_price_full).alias("second_qty"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.min("qty_avg_6_prior").over(w_moved_at_to_date).alias("moved_at_to_date_min_qty_avg_6_prior"),
        F.round(F.avg("unit_price_share_of_operator").over(w_moved_at_to_date), 2).alias("moved_at_to_date_avg_unit_price_share_of_operato"),
        F.round(F.col("qty_tercile") / F.nullif(F.sum("qty_tercile").over(w_moved_at_to_date), F.lit(0)), 4).alias("qty_tercile_share_of_moved_at_to_date"),
        F.lead("qty").over(w_warehouse_id_by_unit_price_s).alias("next_qty"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_filled",
        "next_moved_at",
        "qty_tercile",
        "unit_price_filled_dense_rank",
        "last_qty",
        "qty_avg_6_prior",
        "prev_unit_price_filled",
        "next3_unit_price_filled",
        "qty_vs_operator_avg",
        "seq_no",
        "second_qty",
        "moved_at_to_date_avg_unit_price_share_of_operato",
        "qty_tercile_share_of_moved_at_to_date",
        "next_qty",
        F.abs(F.col("seq_no") - 5).alias("seq_no_distance"),
    )
    .orderBy(
        "qty_tercile",
        F.col("moved_at_to_date_avg_unit_price_share_of_operato").desc(),
    )
)
