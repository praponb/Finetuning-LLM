from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_move_type = Window.partitionBy("operator", "move_type")
w_warehouse_id_by_greatest_uni = Window.partitionBy("warehouse_id").orderBy(F.col("greatest_unit_price_qty").desc(), "id").rowsBetween(-6, Window.currentRow)
w_moved_at_year_operator_by_qt = Window.partitionBy(F.year("moved_at"), "operator").orderBy("qty")
w_moved_at_year_operator_by_qt_2 = Window.partitionBy(F.year("moved_at"), "operator").orderBy("qty", "id")
w_move_type_operator_by_unit_p = Window.partitionBy("move_type", "operator").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_operator_by_unit_p_2 = Window.partitionBy("move_type", "operator").orderBy(F.col("unit_price").desc())
w_move_type_moved_at_month_by = Window.partitionBy("move_type", F.month("moved_at")).orderBy("greatest_unit_price_qty")
w_move_type_operator_by_unit_p_3 = Window.partitionBy("move_type", "operator").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_sku_id_by_rows_in_o = Window.partitionBy("operator", "sku_id").orderBy(F.col("rows_in_operator_move_type").desc(), "id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")

prepared = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp")) & (F.col("move_type") == "INBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
    )
)

metrics = (
    prepared
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "greatest_unit_price_qty",
        F.count(F.lit(1)).over(w_operator_move_type).alias("rows_in_operator_move_type"),
        F.round(F.avg("qty").over(w_warehouse_id_by_greatest_uni), 2).alias("qty_avg_7"),
        F.round(F.cume_dist().over(w_moved_at_year_operator_by_qt), 3).alias("qty_cume_dist"),
        (F.col("qty") - F.lag("qty").over(w_moved_at_year_operator_by_qt_2)).alias("qty_change"),
    )
)

ranked = (
    metrics
    .select(
        "id",
        "qty",
        "operator",
        "unit_price",
        "rows_in_operator_move_type",
        "qty_avg_7",
        "qty_cume_dist",
        F.nth_value("qty_avg_7", 2).over(w_move_type_operator_by_unit_p).alias("second_qty_avg_7"),
        F.rank().over(w_move_type_operator_by_unit_p_2).alias("unit_price_rank"),
        F.round(F.percent_rank().over(w_move_type_moved_at_month_by), 3).alias("greatest_unit_price_qty_percent_rank"),
        F.min("qty_change").over(w_move_type_operator_by_unit_p_3).alias("min_qty_change_so_far"),
        F.dense_rank().over(w_operator_sku_id_by_rows_in_o).alias("rows_in_operator_move_type_dense_rank"),
        F.round(F.col("rows_in_operator_move_type") - F.avg("rows_in_operator_move_type").over(w_sku_id_operator), 2).alias("rows_in_operator_move_type_vs_sku_id_operator_av"),
    )
)

result = (
    ranked
    .groupBy("rows_in_operator_move_type_dense_rank")
    .pivot("operator", ["op_ann", "op_chan", "op_bee", "op_dew"])
    .agg(F.count("rows_in_operator_move_type_vs_sku_id_operator_av"))
)
