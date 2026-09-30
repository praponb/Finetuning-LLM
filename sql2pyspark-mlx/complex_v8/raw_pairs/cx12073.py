from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_qty_t = Window.partitionBy("warehouse_id", "sku_id").orderBy("qty")
w_warehouse_id_by_qty_running = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_moved_at_month = Window.partitionBy(F.month("moved_at"))
w_sku_id_operator_by_moved_at = Window.partitionBy("sku_id", "operator").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_moves = df_stock_moves.filter(F.col("move_type") != "OUTBOUND")

ranked = (
    base_moves
    .select(
        "*",
        F.dense_rank().over(w_warehouse_id_sku_id_by_qty_t).alias("qty_dense_rank"),
        F.sum("qty").over(w_warehouse_id_by_qty_running).alias("running_qty"),
        F.sum("qty").over(w_warehouse_id).alias("warehouse_id_sum_qty"),
        F.dense_rank().over(w_warehouse_id_by_qty).alias("qty_dense_rank_2"),
        F.sum("qty").over(w_moved_at_month).alias("moved_at_month_sum_qty"),
        F.nth_value("unit_price", 2).over(w_sku_id_operator_by_moved_at).alias("second_unit_price"),
        F.max("unit_price").over(w_moved_at_month).alias("moved_at_month_max_unit_price"),
        F.rank().over(w_warehouse_id_sku_id_by_qty_t).alias("qty_rank"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id), 2).alias("qty_vs_warehouse_id_avg"),
        F.sum("unit_price").over(w_warehouse_id).alias("warehouse_id_sum_unit_price"),
    )
)

top_per_group = (
    ranked
    .filter((F.col("qty_rank") <= 5) & (F.col("qty_dense_rank") < 9))
    .drop("qty_rank")
)

totals = (
    top_per_group
    .groupBy("move_type")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("unit_price").alias("total_unit_price"),
        F.round(F.avg("warehouse_id_sum_unit_price"), 2).alias("avg_warehouse_id_sum_unit_price"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.max("moved_at_month_sum_qty").alias("max_moved_at_month_sum_qty_check"),
    )
    .filter(F.col("max_moved_at_month_sum_qty_check") > 501)
    .drop("max_moved_at_month_sum_qty_check")
)

result = (
    totals
    .filter(F.col("avg_warehouse_id_sum_unit_price") > 1250.8)
    .select(
        "move_type",
        "cnt",
        "total_unit_price",
        "avg_warehouse_id_sum_unit_price",
        "operator_list",
        F.when(F.col("cnt") >= 9, "HIGH").when(F.col("cnt") >= 5, "MEDIUM").otherwise("LOW").alias("cnt_tier"),
    )
    .orderBy(F.col("move_type").desc())
)
