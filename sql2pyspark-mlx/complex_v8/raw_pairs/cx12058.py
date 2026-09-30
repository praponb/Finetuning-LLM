from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_move_type_by_moved = Window.partitionBy("operator", "move_type").orderBy(F.col("moved_at").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_warehouse_id_moved_at_to_dat = Window.partitionBy("warehouse_id", F.to_date("moved_at"))
w_sku_id_by_unit_price_running = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_operator = Window.partitionBy("operator")
w_move_type_operator_by_moved = Window.partitionBy("move_type", "operator").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_unit_price_full = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_warehouse_id_mo = Window.partitionBy("move_type").orderBy(F.col("warehouse_id_moved_at_to_date_avg_qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_warehouse_id_mo_2 = Window.partitionBy("move_type").orderBy(F.col("warehouse_id_moved_at_to_date_avg_qty").desc(), "id")
w_move_type = Window.partitionBy("move_type")

prepared = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
    )
)

enriched = (
    prepared.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.operator_prefix",
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "operator_prefix",
        "sku_code",
        "sku_unit_cost",
        F.round(F.avg("sku_unit_cost").over(w_operator_move_type_by_moved), 2).alias("sku_unit_cost_avg_4"),
        F.sum("qty").over(w_warehouse_id_moved_at_to_dat).alias("warehouse_id_moved_at_to_date_sum_qty"),
        F.count(F.lit(1)).over(w_sku_id_by_unit_price_running).alias("running_count"),
        F.rank().over(w_sku_id_by_unit_price).alias("unit_price_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_operator), 2).alias("unit_price_vs_operator_avg"),
        F.nth_value("sku_unit_cost", 2).over(w_move_type_operator_by_moved).alias("second_sku_unit_cost"),
        F.first("qty").over(w_sku_id_by_unit_price_full).alias("first_qty"),
        F.sum("unit_price").over(w_warehouse_id_moved_at_to_dat).alias("warehouse_id_moved_at_to_date_sum_unit_price"),
        F.round(F.avg("qty").over(w_warehouse_id_moved_at_to_dat), 2).alias("warehouse_id_moved_at_to_date_avg_qty"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.max("second_sku_unit_cost").over(w_move_type_by_warehouse_id_mo).alias("max_second_sku_unit_cost_so_far"),
        F.row_number().over(w_move_type_by_warehouse_id_mo_2).alias("warehouse_id_moved_at_to_date_avg_qty_seq"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.ntile(5).over(w_move_type_by_warehouse_id_mo_2).alias("warehouse_id_moved_at_to_date_avg_qty_quintile"),
        F.first("running_count", ignorenulls=True).over(w_move_type_by_warehouse_id_mo).alias("first_known_running_count"),
        F.round(F.col("unit_price_vs_operator_avg") - F.avg("unit_price_vs_operator_avg").over(w_move_type), 2).alias("unit_price_vs_operator_avg_vs_move_type_avg"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "qty",
        "moved_at",
        "operator",
        "operator_prefix",
        "sku_unit_cost",
        "sku_unit_cost_avg_4",
        "warehouse_id_moved_at_to_date_sum_qty",
        "running_count",
        "unit_price_rank",
        "unit_price_vs_operator_avg",
        "warehouse_id_moved_at_to_date_avg_qty",
        "max_second_sku_unit_cost_so_far",
        "warehouse_id_moved_at_to_date_avg_qty_seq",
        "warehouse_id_moved_at_to_date_avg_qty_quintile",
        "first_known_running_count",
        "unit_price_vs_operator_avg_vs_move_type_avg",
    )
)
