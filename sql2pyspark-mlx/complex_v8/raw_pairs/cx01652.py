from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_qty_full = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_opened_on_month_by = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_operator_by_sku = Window.partitionBy("warehouse_id", "operator").orderBy("sku_unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_opened_on_month_by_2 = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("qty").desc(), "id")
w_all_by_qty_running = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_qty_running = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_category_warehouse_id_by = Window.partitionBy("sku_category", "warehouse_id").orderBy(F.col("sku_unit_cost").desc(), "id")
w_moved_at_month = Window.partitionBy(F.month("moved_at"))
w_warehouse_opened_on_month_by_3 = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("qty").desc())
w_operator_warehouse_opened_on = Window.partitionBy("operator", F.date_trunc("month", "warehouse_opened_on"))
w_sku_id_warehouse_id_by_wareh = Window.partitionBy("sku_id", "warehouse_id").orderBy("warehouse_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_moves = (
    df_stock_moves
    .filter((~F.col("move_type").isin("ADJUST", "OUTBOUND")) | (F.col("qty") <= 288))
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
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.nth_value("warehouse_opened_on", 2).over(w_operator_by_qty_full).alias("second_warehouse_opened_on"),
        F.count(F.lit(1)).over(w_warehouse_opened_on_month_by).alias("running_count"),
        F.last("moved_at").over(w_warehouse_id_operator_by_sku).alias("last_moved_at"),
        F.rank().over(w_warehouse_opened_on_month_by_2).alias("qty_rank"),
        F.first("qty", ignorenulls=True).over(w_all_by_qty_running).alias("first_known_qty"),
        F.min("qty").over(w_operator_by_qty_running).alias("min_qty_so_far"),
        F.datediff(F.col("warehouse_opened_on"), F.lag("warehouse_opened_on").over(w_sku_category_warehouse_id_by)).alias("days_since_prev_warehouse_opened_on"),
        F.count(F.lit(1)).over(w_moved_at_month).alias("rows_in_moved_at_month"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_moved_at_month), F.lit(0)), 4).alias("qty_share_of_moved_at_month"),
        F.first("sku_unit_cost").over(w_operator_by_qty_full).alias("first_sku_unit_cost"),
        F.round(F.percent_rank().over(w_warehouse_opened_on_month_by_3), 3).alias("qty_percent_rank"),
        F.round(F.avg("qty").over(w_operator_warehouse_opened_on), 2).alias("operator_warehouse_opened_on_month_avg_qty"),
        F.nth_value("qty", 2).over(w_sku_id_warehouse_id_by_wareh).alias("second_qty"),
    )
)

first_rows = (
    windowed
    .filter(F.col("qty_rank") <= 5)
    .drop("qty_rank")
)

result = (
    first_rows
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "operator",
        "warehouse_opened_on",
        "sku_category",
        "last_moved_at",
        "first_known_qty",
        "min_qty_so_far",
        "days_since_prev_warehouse_opened_on",
        "rows_in_moved_at_month",
        "qty_share_of_moved_at_month",
        "qty_percent_rank",
        "operator_warehouse_opened_on_month_avg_qty",
    )
    .orderBy(F.col("operator").desc())
)
