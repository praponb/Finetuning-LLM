from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_unit = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_all_by_unit_price_ties = Window.orderBy("unit_price")
w_sku_id_moved_at_month = Window.partitionBy("sku_id", F.date_trunc("month", "moved_at"))
w_all_by_unit_price_full = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_sku_id_by_unit_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("unit_price").asc_nulls_last(), "id")
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_moving_6 = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_sku_id_by_unit_3 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("unit_price").asc_nulls_last())

cleaned = (
    df_stock_moves
    .filter((F.col("operator") == "op_bee") | (F.hour(F.col("moved_at")).between(6, 9)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_trends = (
    cleaned
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.avg("unit_price").over(w_warehouse_id_sku_id_by_unit), 2).alias("unit_price_avg_3_prior"),
        F.round(F.percent_rank().over(w_all_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_moved_at_month), 2).alias("unit_price_vs_sku_id_moved_at_month_avg"),
        F.last("moved_at").over(w_all_by_unit_price_full).alias("last_moved_at"),
        F.lead("unit_price", 3).over(w_warehouse_id_sku_id_by_unit_2).alias("next3_unit_price"),
        F.min("unit_price").over(w_sku_id_moved_at_month).alias("sku_id_moved_at_month_min_unit_price"),
        F.size(F.collect_set("move_type").over(w_operator_warehouse_id)).alias("distinct_move_type_in_operator_warehouse_id"),
        F.first("unit_price").over(w_all_by_moved_at_full).alias("first_unit_price"),
        F.round(F.avg("unit_price").over(w_all_by_moved_at_moving_6), 2).alias("unit_price_avg_7"),
        F.last("moved_at", ignorenulls=True).over(w_all_by_unit_price_running).alias("last_known_moved_at"),
        F.rank().over(w_warehouse_id_sku_id_by_unit_3).alias("unit_price_rank"),
    )
)

top_rows = (
    with_trends
    .filter(F.col("unit_price_rank") == 1)
    .drop("unit_price_rank")
)

result = (
    top_rows
    .select(
        "id",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_avg_3_prior",
        "next3_unit_price",
        "distinct_move_type_in_operator_warehouse_id",
        "last_known_moved_at",
        F.concat_ws(": ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.least(F.col("qty"), F.col("unit_price_avg_3_prior")).alias("least_qty_unit_price_avg_3_prior"),
    )
    .orderBy("moved_at")
)
