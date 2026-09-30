from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_all_by_qty = Window.orderBy(F.col("qty").desc_nulls_first(), "id")
w_moved_at_to_date_sku_id = Window.partitionBy(F.to_date("moved_at"), "sku_id")
w_sku_id_by_unit_price_adj = Window.partitionBy("sku_id").orderBy("unit_price_adj", "id")
w_all_by_unit_price_adj = Window.orderBy(F.col("unit_price_adj").desc_nulls_first(), "id")
w_all_by_unit_price_adj_moving = Window.orderBy(F.col("unit_price_adj").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_unit_price_adj_runnin = Window.orderBy(F.col("unit_price_adj").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_unit_price_adj_pri = Window.partitionBy("sku_id").orderBy("unit_price_adj", "id").rowsBetween(-2, -1)
w_moved_at_to_date = Window.partitionBy(F.to_date("moved_at"))
w_moved_at_to_date_warehouse_i = Window.partitionBy(F.to_date("moved_at"), "warehouse_id").orderBy(F.col("moved_at_to_date_sku_id_min_qty").desc())

filtered = (
    df_stock_moves
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.min("unit_price_adj").over(w_sku_id_operator).alias("sku_id_operator_min_unit_price_adj"),
        F.lag("unit_price_adj").over(w_all_by_qty).alias("prev_unit_price_adj"),
        F.min("qty").over(w_moved_at_to_date_sku_id).alias("moved_at_to_date_sku_id_min_qty"),
        F.min("qty").over(w_sku_id_operator).alias("sku_id_operator_min_qty"),
        F.dense_rank().over(w_sku_id_by_unit_price_adj).alias("unit_price_adj_dense_rank"),
        (F.col("unit_price_adj") - F.lag("unit_price_adj").over(w_all_by_unit_price_adj)).alias("unit_price_adj_change"),
        F.sum("unit_price_adj").over(w_moved_at_to_date_sku_id).alias("moved_at_to_date_sku_id_sum_unit_price_adj"),
        F.sum("qty").over(w_all_by_unit_price_adj_moving).alias("qty_sum_7"),
        F.count(F.lit(1)).over(w_sku_id_operator).alias("rows_in_sku_id_operator"),
        F.round(F.percent_rank().over(w_sku_id_by_unit_price_adj), 3).alias("unit_price_adj_percent_rank"),
        F.count(F.lit(1)).over(w_all_by_unit_price_adj_runnin).alias("running_count"),
        F.max("qty").over(w_sku_id_by_unit_price_adj_pri).alias("qty_max_2_prior"),
    )
)

top_per_group = (
    with_trends
    .filter((F.col("unit_price_adj_dense_rank") <= 2) & (F.col("running_count") < 16))
    .drop("unit_price_adj_dense_rank")
)

with_windows = (
    top_per_group
    .select(
        "*",
        F.round(F.col("unit_price_adj") - F.avg("unit_price_adj").over(w_moved_at_to_date), 2).alias("unit_price_adj_vs_moved_at_to_date_avg"),
        F.round(F.percent_rank().over(w_moved_at_to_date_warehouse_i), 3).alias("moved_at_to_date_sku_id_min_qty_percent_rank"),
        F.sum("qty").over(w_moved_at_to_date).alias("moved_at_to_date_sum_qty"),
        F.min("unit_price_adj").over(w_moved_at_to_date).alias("moved_at_to_date_min_unit_price_adj"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price_adj",
        "sku_id_operator_min_unit_price_adj",
        "prev_unit_price_adj",
        "moved_at_to_date_sku_id_min_qty",
        "sku_id_operator_min_qty",
        "unit_price_adj_change",
        "qty_sum_7",
        "rows_in_sku_id_operator",
        "unit_price_adj_percent_rank",
        "running_count",
        "qty_max_2_prior",
        "unit_price_adj_vs_moved_at_to_date_avg",
        "moved_at_to_date_sku_id_min_qty_percent_rank",
        "moved_at_to_date_sum_qty",
        "moved_at_to_date_min_unit_price_adj",
        F.concat_ws(": ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
    .orderBy("running_count", "moved_at_to_date_min_unit_price_adj", "id")
    .limit(10)
)
