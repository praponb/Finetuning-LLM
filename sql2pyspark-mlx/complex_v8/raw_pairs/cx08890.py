from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_filled = Window.orderBy("qty_filled", "id")
w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator = Window.partitionBy("operator")
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_warehouse_id_move_type_by_mo_2 = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at", "id")
w_warehouse_id_move_type_by_mo_3 = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at")
w_all_by_moved_at_day_moving_1 = Window.orderBy(F.col("moved_at_day").desc(), "id").rowsBetween(-1, Window.currentRow)
w_move_type_running_qty_filled = Window.partitionBy("move_type", "running_qty_filled_tier").orderBy(F.col("moved_at_rank").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_running_qty_filled_2 = Window.partitionBy("move_type", "running_qty_filled_tier").orderBy(F.col("moved_at_rank").desc_nulls_first(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_prev3_qty_filled_ties = Window.orderBy(F.col("prev3_qty_filled").desc())

scoped = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_chan", "op_eve"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.row_number().over(w_all_by_qty_filled).alias("qty_filled_seq"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
        F.sum("qty_filled").over(w_warehouse_id_move_type_by_mo).alias("running_qty_filled"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator), F.lit(0)), 4).alias("unit_price_share_of_operator"),
        F.lag("qty_filled", 3).over(w_sku_id_by_unit_price).alias("prev3_qty_filled"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_moved_at)).alias("unit_price_change"),
        F.rank().over(w_warehouse_id_move_type_by_mo_2).alias("moved_at_rank"),
        F.first("qty_filled", ignorenulls=True).over(w_warehouse_id_move_type_by_mo).alias("first_known_qty_filled"),
        F.dense_rank().over(w_warehouse_id_move_type_by_mo_3).alias("moved_at_dense_rank"),
        F.lead("moved_at").over(w_warehouse_id_move_type_by_mo_2).alias("next_moved_at"),
        F.min("qty_filled").over(w_operator).alias("operator_min_qty_filled"),
    )
)

tagged = (
    metrics
    .filter(F.col("next_moved_at") < F.lit("2025-02-24 08:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator_move_type_label",
        "qty_filled_seq",
        "distinct_warehouse_id_in_sku_id",
        "running_qty_filled",
        "prev3_qty_filled",
        "unit_price_change",
        "moved_at_rank",
        "first_known_qty_filled",
        "moved_at_dense_rank",
        "next_moved_at",
        "operator_min_qty_filled",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
        F.when(F.col("running_qty_filled") >= 3000, "large").when(F.col("running_qty_filled") >= 2251, "medium").otherwise("small").alias("running_qty_filled_tier"),
    )
)

with_windows = (
    tagged
    .select(
        "*",
        F.max("prev3_qty_filled").over(w_all_by_moved_at_day_moving_1).alias("prev3_qty_filled_max_2"),
        F.nth_value("next_moved_at", 2).over(w_move_type_running_qty_filled).alias("second_next_moved_at"),
        F.row_number().over(w_move_type_running_qty_filled_2).alias("seq_no"),
        F.round(F.col("first_known_qty_filled") - F.avg("first_known_qty_filled").over(w_warehouse_id), 2).alias("first_known_qty_filled_vs_warehouse_id_avg"),
        F.round(F.col("running_qty_filled") - F.avg("running_qty_filled").over(w_warehouse_id), 2).alias("running_qty_filled_vs_warehouse_id_avg"),
        F.sum("unit_price_change").over(w_warehouse_id).alias("warehouse_id_sum_unit_price_change"),
        F.round(F.cume_dist().over(w_all_by_prev3_qty_filled_ties), 3).alias("prev3_qty_filled_cume_dist"),
        F.lead("next_moved_at").over(w_move_type_running_qty_filled_2).alias("next_next_moved_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator_move_type_label",
        "distinct_warehouse_id_in_sku_id",
        "running_qty_filled",
        "prev3_qty_filled",
        "unit_price_change",
        "moved_at_rank",
        "first_known_qty_filled",
        "moved_at_dense_rank",
        "next_moved_at",
        "operator_min_qty_filled",
        "prev3_qty_filled_max_2",
        "second_next_moved_at",
        "seq_no",
        "running_qty_filled_vs_warehouse_id_avg",
        "prev3_qty_filled_cume_dist",
        F.concat_ws(" / ", F.col("operator_move_type_label"), F.col("move_type")).alias("operator_move_type_label_move_type_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("next_next_moved_at")).alias("days_since_next_next_moved_at"),
    )
    .orderBy("prev3_qty_filled")
)
