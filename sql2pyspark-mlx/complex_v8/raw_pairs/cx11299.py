from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_to_date = Window.partitionBy(F.to_date("moved_at"))
w_all_by_warehouse_opened_on = Window.orderBy(F.col("warehouse_opened_on").desc(), "id")
w_sku_id_by_qty_distance_ties = Window.partitionBy("sku_id").orderBy("qty_distance")
w_sku_id_by_qty_distance_full = Window.partitionBy("sku_id").orderBy("qty_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_opened_on_year_sku = Window.partitionBy(F.year("warehouse_opened_on"), "sku_id")
w_operator_by_qty_full = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_warehouse_opened_on_m = Window.partitionBy(F.month("second_warehouse_opened_on"), "distinct_sku_id_in_moved_at_to_date")
w_moved_at_year_warehouse_id = Window.partitionBy(F.year("moved_at"), "warehouse_id")
w_all_by_second_warehouse_open = Window.orderBy(F.col("second_warehouse_opened_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_distinct_sku_id_in = Window.partitionBy("operator", "distinct_sku_id_in_moved_at_to_date").orderBy(F.col("unit_price").desc(), "id")
w_all_by_second_warehouse_open_2 = Window.orderBy(F.col("second_warehouse_opened_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_stock_moves
    .filter(F.col("unit_price") >= 111.7)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 230).alias("qty_distance"),
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
    )
)

combined_details_warehouse = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

with_trends = (
    combined_details_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "qty_distance",
        "warehouse_wh_code",
        F.sum("unit_price").over(w_moved_at_to_date).alias("moved_at_to_date_sum_unit_price"),
        F.row_number().over(w_all_by_warehouse_opened_on).alias("rn"),
        F.round(F.percent_rank().over(w_sku_id_by_qty_distance_ties), 3).alias("qty_distance_percent_rank"),
        F.nth_value("warehouse_opened_on", 2).over(w_sku_id_by_qty_distance_full).alias("second_warehouse_opened_on"),
        F.round(F.cume_dist().over(w_sku_id_by_qty_distance_ties), 3).alias("qty_distance_cume_dist"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_opened_on_year_sku), F.lit(0)), 4).alias("qty_share_of_warehouse_opened_on_year_sku_id"),
        F.count(F.lit(1)).over(w_warehouse_opened_on_year_sku).alias("rows_in_warehouse_opened_on_year_sku_id"),
        F.size(F.collect_set("sku_id").over(w_moved_at_to_date)).alias("distinct_sku_id_in_moved_at_to_date"),
        F.nth_value("qty", 2).over(w_operator_by_qty_full).alias("second_qty"),
    )
)

leaders = (
    with_trends
    .filter((F.col("rn") <= 3) & (F.col("qty_distance") > 75))
    .drop("rn")
)

with_windows = (
    leaders
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "qty_distance",
        "moved_at_to_date_sum_unit_price",
        "qty_distance_percent_rank",
        "second_warehouse_opened_on",
        "qty_distance_cume_dist",
        "qty_share_of_warehouse_opened_on_year_sku_id",
        "distinct_sku_id_in_moved_at_to_date",
        "second_qty",
        F.size(F.collect_set("sku_id").over(w_second_warehouse_opened_on_m)).alias("distinct_sku_id_in_second_warehouse_opened_on_mo"),
        F.size(F.collect_set("warehouse_id").over(w_second_warehouse_opened_on_m)).alias("distinct_warehouse_id_in_second_warehouse_opened"),
        F.round(F.col("qty_distance_percent_rank") - F.avg("qty_distance_percent_rank").over(w_moved_at_year_warehouse_id), 2).alias("qty_distance_percent_rank_vs_moved_at_year_wareh"),
        F.sum("qty_distance_percent_rank").over(w_all_by_second_warehouse_open).alias("running_qty_distance_percent_rank"),
        F.lag("qty_distance_cume_dist", 2).over(w_operator_distinct_sku_id_in).alias("prev2_qty_distance_cume_dist"),
        F.first("qty_distance_cume_dist").over(w_all_by_second_warehouse_open_2).alias("first_qty_distance_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "qty_distance",
        "moved_at_to_date_sum_unit_price",
        "qty_distance_percent_rank",
        "qty_distance_cume_dist",
        "qty_share_of_warehouse_opened_on_year_sku_id",
        "second_qty",
        "distinct_sku_id_in_second_warehouse_opened_on_mo",
        "distinct_warehouse_id_in_second_warehouse_opened",
        "running_qty_distance_percent_rank",
        "prev2_qty_distance_cume_dist",
        "first_qty_distance_cume_dist",
        F.when(F.col("operator").isin("op_eve", "op_ann"), "group_a").when(F.col("operator") == "op_chan", "group_b").otherwise("other").alias("operator_group"),
    )
    .orderBy(F.col("first_qty_distance_cume_dist").desc())
)
