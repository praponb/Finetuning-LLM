from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_moved_at_week_to_date_move_t = Window.partitionBy(F.to_date("moved_at_week"), "move_type").orderBy(F.col("moved_at_week").desc(), "id")
w_moved_at_week_to_date_move_t_2 = Window.partitionBy(F.to_date("moved_at_week"), "move_type").orderBy(F.col("moved_at_week").desc())
w_move_type_moved_at_week_mont = Window.partitionBy("move_type", F.month("moved_at_week")).orderBy(F.col("moved_at_week").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())
w_moved_at_week_year_operator = Window.partitionBy(F.year("moved_at_week"), "operator").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id")
w_sku_id_next_moved_at_month = Window.partitionBy("sku_id", F.month("next_moved_at"))
w_sku_id_by_last_qty_full = Window.partitionBy("sku_id").orderBy("last_qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next_moved_at_year_by_prev_q = Window.partitionBy(F.year("next_moved_at")).orderBy(F.col("prev_qty").desc(), "id").rowsBetween(-1, Window.currentRow)

recent_moves = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")) | (F.col("unit_price").between(37.6, 341.5)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
    )
)

metrics = (
    recent_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "move_type_prefix",
        "moved_at_week",
        F.min("qty").over(w_warehouse_id_operator).alias("warehouse_id_operator_min_qty"),
        F.lead("moved_at").over(w_all_by_qty).alias("next_moved_at"),
        F.count(F.lit(1)).over(w_warehouse_id_operator).alias("rows_in_warehouse_id_operator"),
        F.lag("qty").over(w_moved_at_week_to_date_move_t).alias("prev_qty"),
        F.rank().over(w_moved_at_week_to_date_move_t_2).alias("moved_at_week_rank"),
        F.max("qty").over(w_warehouse_id_operator).alias("warehouse_id_operator_max_qty"),
        F.nth_value("moved_at_week", 2).over(w_move_type_moved_at_week_mont).alias("second_moved_at_week"),
        F.lag("moved_at_week").over(w_moved_at_week_to_date_move_t).alias("prev_moved_at_week"),
        F.round(F.percent_rank().over(w_all_by_qty_ties), 3).alias("qty_percent_rank"),
        F.last("qty").over(w_moved_at_week_year_operator).alias("last_qty"),
        F.rank().over(w_sku_id_by_moved_at).alias("moved_at_rank"),
    )
)

leaders = (
    metrics
    .filter(F.col("moved_at_rank") == 1)
    .drop("moved_at_rank")
)

ranked = (
    leaders
    .select(
        "*",
        F.max("moved_at_week_rank").over(w_sku_id_next_moved_at_month).alias("sku_id_next_moved_at_month_max_moved_at_week_ran"),
        F.nth_value("moved_at", 2).over(w_sku_id_by_last_qty_full).alias("second_moved_at"),
        F.round(F.avg("qty_percent_rank").over(w_next_moved_at_year_by_prev_q), 2).alias("qty_percent_rank_avg_2"),
        F.round(F.col("moved_at_week_rank") / F.nullif(F.sum("moved_at_week_rank").over(w_sku_id_next_moved_at_month), F.lit(0)), 4).alias("moved_at_week_rank_share_of_sku_id_next_moved_at"),
    )
)

result = (
    ranked
    .filter(F.col("operator").isin("op_bee", "op_eve"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "move_type_prefix",
        "warehouse_id_operator_min_qty",
        "rows_in_warehouse_id_operator",
        "moved_at_week_rank",
        "warehouse_id_operator_max_qty",
        "prev_moved_at_week",
        "qty_percent_rank",
        "last_qty",
        "second_moved_at",
        "qty_percent_rank_avg_2",
        "moved_at_week_rank_share_of_sku_id_next_moved_at",
    )
    .orderBy(F.col("move_type").desc(), "id")
    .limit(20)
)
