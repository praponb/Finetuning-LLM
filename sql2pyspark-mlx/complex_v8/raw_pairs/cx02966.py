from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_opened_on_month_by = Window.partitionBy(F.month("warehouse_opened_on")).orderBy(F.col("moved_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_warehouse_id_by_moved = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("moved_at_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_warehouse_op = Window.partitionBy("warehouse_id").orderBy("warehouse_opened_on", "id")
w_sku_id_warehouse_id_by_moved_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("moved_at_year").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_sku_id_warehouse_id_by_moved_3 = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("moved_at_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_warehouse_op_2 = Window.partitionBy("warehouse_id").orderBy("warehouse_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_year_to_date = Window.partitionBy(F.to_date("moved_at_year"))
w_sku_id_move_type_by_qty = Window.partitionBy("sku_id", "move_type").orderBy(F.col("qty").desc(), "id")
w_sku_id_move_type_by_qty_prio = Window.partitionBy("sku_id", "move_type").orderBy(F.col("qty").desc(), "id").rowsBetween(-1, -1)
w_move_type_sku_id = Window.partitionBy("move_type", "sku_id")
w_sku_id_move_type_by_qty_full = Window.partitionBy("sku_id", "move_type").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_max_qty_so_f = Window.partitionBy("warehouse_id").orderBy(F.col("max_qty_so_far").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id = Window.partitionBy("sku_id")
w_operator_group = Window.partitionBy("operator_group")
w_all_by_running_unit_price = Window.orderBy("running_unit_price", "id")
w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy("moved_at_year", "id")
w_prev3_warehouse_opened_on_mo = Window.partitionBy(F.date_trunc("month", "prev3_warehouse_opened_on"), "sku_id").orderBy("prev_unit_price")

base = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
    )
)

enriched = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select("s.*", F.col("w.opened_on").alias("warehouse_opened_on"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.last("unit_price", ignorenulls=True).over(w_warehouse_opened_on_month_by).alias("last_known_unit_price"),
        F.sum("unit_price").over(w_sku_id_warehouse_id_by_moved).alias("running_unit_price"),
        F.lag("unit_price", 1, 0).over(w_warehouse_id_by_warehouse_op).alias("prev_unit_price"),
        F.round(F.avg("qty").over(w_sku_id_warehouse_id_by_moved_2), 2).alias("qty_avg_2"),
        F.last("warehouse_opened_on").over(w_sku_id_warehouse_id_by_moved_3).alias("last_warehouse_opened_on"),
        F.max("unit_price").over(w_warehouse_id_by_warehouse_op_2).alias("max_unit_price_so_far"),
        F.count(F.lit(1)).over(w_moved_at_year_to_date).alias("rows_in_moved_at_year_to_date"),
        F.lag("warehouse_opened_on", 3).over(w_sku_id_move_type_by_qty).alias("prev3_warehouse_opened_on"),
        F.round(F.avg("qty").over(w_sku_id_move_type_by_qty_prio), 2).alias("qty_avg_1_prior"),
        F.count(F.lit(1)).over(w_move_type_sku_id).alias("rows_in_move_type_sku_id"),
        F.min("unit_price").over(w_warehouse_id_by_warehouse_op_2).alias("min_unit_price_so_far"),
        F.size(F.collect_set("warehouse_id").over(w_move_type_sku_id)).alias("distinct_warehouse_id_in_move_type_sku_id"),
        F.nth_value("qty", 2).over(w_sku_id_move_type_by_qty_full).alias("second_qty"),
        F.max("qty").over(w_warehouse_opened_on_month_by).alias("max_qty_so_far"),
        F.round(F.avg("unit_price").over(w_move_type_sku_id), 2).alias("move_type_sku_id_avg_unit_price"),
    )
)

tagged = (
    with_windows
    .filter(F.col("running_unit_price") < 3000.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "moved_at_year",
        "warehouse_opened_on",
        "running_unit_price",
        "prev_unit_price",
        "qty_avg_2",
        "last_warehouse_opened_on",
        "prev3_warehouse_opened_on",
        "qty_avg_1_prior",
        "rows_in_move_type_sku_id",
        "min_unit_price_so_far",
        "max_qty_so_far",
        "move_type_sku_id_avg_unit_price",
        F.coalesce(F.col("qty_avg_2"), F.lit(0.0)).alias("qty_avg_2_filled"),
        F.when(F.col("operator").isin("op_bee", "op_chan"), "group_a").when(F.col("operator") == "op_eve", "group_b").otherwise("other").alias("operator_group"),
        F.abs(F.col("qty_avg_1_prior") - 350.3).alias("qty_avg_1_prior_distance"),
    )
)

metrics = (
    tagged
    .select(
        "*",
        F.sum("min_unit_price_so_far").over(w_warehouse_id_by_max_qty_so_f).alias("running_min_unit_price_so_far"),
        F.sum("qty_avg_1_prior_distance").over(w_sku_id).alias("sku_id_sum_qty_avg_1_prior_distance"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id), 2).alias("qty_vs_sku_id_avg"),
        F.size(F.collect_set("warehouse_id").over(w_operator_group)).alias("distinct_warehouse_id_in_operator_group"),
        F.lead("warehouse_opened_on").over(w_all_by_running_unit_price).alias("next_warehouse_opened_on"),
        F.dense_rank().over(w_warehouse_id_sku_id_by_moved).alias("moved_at_year_dense_rank"),
        F.count(F.lit(1)).over(w_warehouse_id_by_max_qty_so_f).alias("running_count"),
        F.round(F.cume_dist().over(w_prev3_warehouse_opened_on_mo), 3).alias("prev_unit_price_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "warehouse_opened_on",
        "running_unit_price",
        "prev_unit_price",
        "qty_avg_2",
        "last_warehouse_opened_on",
        "prev3_warehouse_opened_on",
        "min_unit_price_so_far",
        "max_qty_so_far",
        "move_type_sku_id_avg_unit_price",
        "qty_avg_2_filled",
        "operator_group",
        "qty_avg_1_prior_distance",
        "sku_id_sum_qty_avg_1_prior_distance",
        "qty_vs_sku_id_avg",
        "moved_at_year_dense_rank",
        "running_count",
    )
    .orderBy("sku_id")
)
