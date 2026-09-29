from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_moved_at_tie = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc())
w_moved_at_month_warehouse_id = Window.partitionBy(F.date_trunc("month", "moved_at"), "warehouse_id").orderBy(F.col("moved_at").desc_nulls_first(), "id")
w_move_type_warehouse_id_by_mo = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("moved_at").desc())
w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at", "id")
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_move_type_warehouse_id_by_mo_2 = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_move_type_warehouse_id_by_mo_3 = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("moved_at").desc(), "id")
w_moved_at_month_warehouse_id_2 = Window.partitionBy(F.date_trunc("month", "moved_at"), "warehouse_id").orderBy(F.col("moved_at").desc_nulls_first())
w_operator_by_next3_moved_at_p = Window.partitionBy("operator").orderBy("next3_moved_at", "id").rowsBetween(-3, -1)
w_operator = Window.partitionBy("operator")
w_moved_at_rank_by_moved_at_ra = Window.partitionBy("moved_at_rank").orderBy(F.col("moved_at_rank").desc(), "id")
w_sku_id = Window.partitionBy("sku_id")
w_moved_at_to_date_by_last_mov = Window.partitionBy(F.to_date("moved_at")).orderBy(F.col("last_moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_rank_by_moved_at_ra_2 = Window.partitionBy("moved_at_rank").orderBy(F.col("moved_at_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.round(F.percent_rank().over(w_warehouse_id_by_moved_at_tie), 3).alias("moved_at_percent_rank"),
        F.round(F.cume_dist().over(w_moved_at_month_warehouse_id), 3).alias("moved_at_cume_dist"),
        F.dense_rank().over(w_move_type_warehouse_id_by_mo).alias("moved_at_dense_rank"),
        F.ntile(4).over(w_warehouse_id_move_type_by_mo).alias("moved_at_quartile"),
        F.row_number().over(w_warehouse_id_by_moved_at).alias("seq_no"),
        F.last("moved_at").over(w_move_type_warehouse_id_by_mo_2).alias("last_moved_at"),
        F.round(F.percent_rank().over(w_all_by_moved_at), 3).alias("moved_at_percent_rank_2"),
        F.round(F.cume_dist().over(w_all_by_moved_at_ties), 3).alias("moved_at_cume_dist_2"),
        F.lead("moved_at", 3).over(w_move_type_warehouse_id_by_mo_3).alias("next3_moved_at"),
        F.rank().over(w_moved_at_month_warehouse_id_2).alias("moved_at_rank"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.max("moved_at_rank").over(w_operator_by_next3_moved_at_p).alias("moved_at_rank_max_3_prior"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
        F.round((F.unix_timestamp("last_moved_at") - F.unix_timestamp(F.lag("last_moved_at").over(w_moved_at_rank_by_moved_at_ra))) / 60, 1).alias("minutes_since_prev_last_moved_at"),
        F.round(F.col("moved_at_dense_rank") - F.avg("moved_at_dense_rank").over(w_sku_id), 2).alias("moved_at_dense_rank_vs_sku_id_avg"),
        F.first("moved_at_dense_rank").over(w_moved_at_to_date_by_last_mov).alias("first_moved_at_dense_rank"),
        F.first("moved_at_percent_rank", ignorenulls=True).over(w_moved_at_rank_by_moved_at_ra_2).alias("first_known_moved_at_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "moved_at_percent_rank",
        "moved_at_cume_dist",
        "moved_at_dense_rank",
        "moved_at_quartile",
        "seq_no",
        "moved_at_cume_dist_2",
        "next3_moved_at",
        "moved_at_rank",
        "moved_at_rank_max_3_prior",
        "rows_in_operator",
        "moved_at_dense_rank_vs_sku_id_avg",
        "first_moved_at_dense_rank",
        "first_known_moved_at_percent_rank",
    )
    .orderBy(F.col("moved_at_rank_max_3_prior").desc(), "seq_no", "id")
    .limit(5)
)
