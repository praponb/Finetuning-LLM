from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_sku_id_by_moved_at_prior_3 = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(-3, -1)
w_all_by_unit_price_prior_2 = Window.orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_operator_moved_at_year = Window.partitionBy("operator", F.year("moved_at"))
w_all_by_qty_distance_full = Window.orderBy(F.col("qty_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty_distance_running = Window.orderBy(F.col("qty_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy("moved_at", "id")

filtered = (
    df_stock_moves
    .filter(F.col("unit_price") > 236.5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 139).alias("qty_distance"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator_sku_id), F.lit(0)), 4).alias("unit_price_share_of_operator_sku_id"),
        F.round(F.avg("qty_distance").over(w_sku_id_by_moved_at_prior_3), 2).alias("qty_distance_avg_3_prior"),
        F.sum("qty_distance").over(w_all_by_unit_price_prior_2).alias("qty_distance_sum_2_prior"),
        F.round(F.col("qty_distance") / F.nullif(F.sum("qty_distance").over(w_operator_moved_at_year), F.lit(0)), 4).alias("qty_distance_share_of_operator_moved_at_year"),
        F.max("qty_distance").over(w_operator_sku_id).alias("operator_sku_id_max_qty_distance"),
        F.nth_value("moved_at", 2).over(w_all_by_qty_distance_full).alias("second_moved_at"),
        F.count(F.lit(1)).over(w_operator_sku_id).alias("rows_in_operator_sku_id"),
        F.first("qty_distance", ignorenulls=True).over(w_all_by_qty_distance_running).alias("first_known_qty_distance"),
        F.row_number().over(w_sku_id_by_moved_at).alias("rn"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "qty_distance",
        "unit_price_share_of_operator_sku_id",
        "qty_distance_avg_3_prior",
        "qty_distance_sum_2_prior",
        "qty_distance_share_of_operator_moved_at_year",
        "operator_sku_id_max_qty_distance",
        "second_moved_at",
        "rows_in_operator_sku_id",
    )
    .orderBy("rows_in_operator_sku_id")
)
