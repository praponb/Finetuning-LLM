from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_moving_2 = Window.orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w_move_type = Window.partitionBy("move_type")
w_all_by_qty_ties = Window.orderBy("qty")
w_all_by_qty = Window.orderBy("qty", "id")
w_all_by_qty_running = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_least_unit_price_qty = Window.orderBy("least_unit_price_qty")
w_all_by_least_unit_price_qty_2 = Window.orderBy("least_unit_price_qty", "id").rowsBetween(-2, Window.currentRow)
w_all_by_unit_price_prior_6 = Window.orderBy("unit_price", "id").rowsBetween(-6, -1)
w_all_by_least_unit_price_qty_3 = Window.orderBy("least_unit_price_qty", "id").rowsBetween(-3, -1)
w_all_by_qty_moving_2 = Window.orderBy("qty", "id").rowsBetween(-2, Window.currentRow)

recent = (
    df_stock_moves
    .filter((F.col("warehouse_id") >= 3) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

with_windows = (
    recent
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "least_unit_price_qty",
        "operator_clean",
        F.round(F.avg("least_unit_price_qty").over(w_all_by_unit_price_moving_2), 2).alias("least_unit_price_qty_avg_3"),
        F.round(F.col("qty") - F.avg("qty").over(w_move_type), 2).alias("qty_vs_move_type_avg"),
        F.rank().over(w_all_by_qty_ties).alias("qty_rank"),
        F.round(F.col("least_unit_price_qty") / F.nullif(F.sum("least_unit_price_qty").over(w_move_type), F.lit(0)), 4).alias("least_unit_price_qty_share_of_move_type"),
        F.dense_rank().over(w_all_by_qty).alias("qty_dense_rank"),
        F.size(F.collect_set("sku_id").over(w_move_type)).alias("distinct_sku_id_in_move_type"),
        F.last("unit_price", ignorenulls=True).over(w_all_by_qty_running).alias("last_known_unit_price"),
        F.rank().over(w_all_by_least_unit_price_qty).alias("least_unit_price_qty_rank"),
        F.ntile(3).over(w_all_by_qty).alias("qty_tercile"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.round(F.avg("least_unit_price_qty").over(w_all_by_least_unit_price_qty_2), 2).alias("least_unit_price_qty_avg_3_2"),
        F.max("qty").over(w_all_by_unit_price_prior_6).alias("qty_max_6_prior"),
        F.round(F.avg("unit_price").over(w_all_by_least_unit_price_qty_3), 2).alias("unit_price_avg_3_prior"),
        F.sum("least_unit_price_qty").over(w_all_by_qty_moving_2).alias("least_unit_price_qty_sum_3"),
    )
)

leaders = with_windows.filter(F.col("qty_rank") <= 3)

result = (
    leaders
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "least_unit_price_qty",
        "operator_clean",
        "least_unit_price_qty_avg_3",
        "qty_rank",
        "qty_dense_rank",
        "distinct_sku_id_in_move_type",
        "last_known_unit_price",
        "qty_tercile",
        "rows_in_move_type",
        "least_unit_price_qty_avg_3_2",
        "qty_max_6_prior",
        "unit_price_avg_3_prior",
    )
    .orderBy(F.col("qty_tercile").asc())
)
