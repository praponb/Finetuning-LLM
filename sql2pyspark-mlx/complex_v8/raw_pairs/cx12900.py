from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_moved_at_ties = Window.partitionBy("operator").orderBy("moved_at")
w_move_type = Window.partitionBy("move_type")
w_moved_at_month_warehouse_id = Window.partitionBy(F.date_trunc("month", "moved_at"), "warehouse_id")
w_operator_by_moved_at_moving = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_qty_running = Window.orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_unit_price_tier_by = Window.partitionBy("move_type", "unit_price_tier").orderBy(F.col("qty").asc_nulls_last(), "id")
w_move_type_unit_price_tier_by_2 = Window.partitionBy("move_type", "unit_price_tier").orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_moved_at = Window.partitionBy("operator").orderBy("moved_at", "id")

valid_moves = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_eve", "op_dew"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 361.9, "HIGH").when(F.col("unit_price") >= 125.9, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
    )
)

metrics = (
    valid_moves
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_tier",
        F.round(F.percent_rank().over(w_operator_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type), 2).alias("unit_price_vs_move_type_avg"),
        F.max("unit_price").over(w_moved_at_month_warehouse_id).alias("moved_at_month_warehouse_id_max_unit_price"),
        F.sum("qty").over(w_operator_by_moved_at_moving).alias("qty_sum_3"),
        F.count(F.lit(1)).over(w_all_by_qty_running).alias("running_count"),
        F.max("unit_price").over(w_move_type).alias("move_type_max_unit_price"),
        F.lead("qty").over(w_move_type_unit_price_tier_by).alias("next_qty"),
        F.min("qty").over(w_move_type_unit_price_tier_by_2).alias("min_qty_so_far"),
        F.row_number().over(w_operator_by_moved_at).alias("moved_at_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_tier",
        "moved_at_percent_rank",
        "unit_price_vs_move_type_avg",
        "moved_at_month_warehouse_id_max_unit_price",
        "qty_sum_3",
        "move_type_max_unit_price",
        "next_qty",
        "min_qty_so_far",
        "moved_at_seq",
    )
    .orderBy(F.col("moved_at").desc(), F.col("moved_at_seq").desc())
)
