from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_group_by_moved_at_t = Window.partitionBy("operator_group").orderBy(F.col("moved_at").asc_nulls_last())
w_sku_id_warehouse_id_by_qty_f = Window.partitionBy("sku_id", "warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_warehouse_id_by_qty = Window.partitionBy("sku_id", "warehouse_id").orderBy("qty", "id")
w_operator_group_by_moved_at_r = Window.partitionBy("operator_group").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_operator_group_by_moved_at = Window.partitionBy("operator_group").orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_warehouse_id_operator_group = Window.partitionBy("warehouse_id", "operator_group").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_warehouse_id_by_qty_t = Window.partitionBy("sku_id", "warehouse_id").orderBy("qty")
w_sku_id_warehouse_id_by_qty_r = Window.partitionBy("sku_id", "warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_operator_group_2 = Window.partitionBy("warehouse_id", "operator_group").orderBy("qty", "id")
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_operator_group_warehouse_id = Window.partitionBy("operator_group", "warehouse_id").orderBy("unit_price")
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_operator_group_3 = Window.partitionBy("warehouse_id", "operator_group").orderBy("running_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_operator_group_4 = Window.partitionBy("warehouse_id", "operator_group").orderBy("running_unit_price", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.when(F.col("operator").isin("op_eve", "op_ann"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.round(F.cume_dist().over(w_operator_group_by_moved_at_t), 3).alias("moved_at_cume_dist"),
        F.last("unit_price").over(w_sku_id_warehouse_id_by_qty_f).alias("last_unit_price"),
        (F.col("unit_price") - F.lag("unit_price").over(w_sku_id_warehouse_id_by_qty)).alias("unit_price_change"),
        F.sum("unit_price").over(w_operator_group_by_moved_at_r).alias("running_unit_price"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.size(F.collect_set("operator_group").over(w_warehouse_id)).alias("distinct_operator_group_in_warehouse_id"),
        F.round(F.percent_rank().over(w_operator_group_by_moved_at), 3).alias("moved_at_percent_rank"),
        F.sum("qty").over(w_warehouse_id_operator_group).alias("running_qty"),
        F.round(F.percent_rank().over(w_sku_id_warehouse_id_by_qty_t), 3).alias("qty_percent_rank"),
        F.round(F.percent_rank().over(w_operator_group_by_moved_at_t), 3).alias("moved_at_percent_rank_2"),
        F.sum("unit_price").over(w_sku_id_warehouse_id_by_qty_r).alias("running_unit_price_2"),
        (F.col("qty") - F.lag("qty").over(w_warehouse_id_operator_group_2)).alias("qty_change"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.rank().over(w_all_by_moved_at).alias("moved_at_rank"),
        F.round(F.cume_dist().over(w_operator_group_warehouse_id), 3).alias("unit_price_cume_dist"),
        F.last("moved_at").over(w_all_by_moved_at_full).alias("last_moved_at"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.nth_value("running_unit_price_2", 2).over(w_warehouse_id_operator_group_3).alias("second_running_unit_price_2"),
        F.sum("running_unit_price").over(w_warehouse_id_operator_group_4).alias("running_unit_price_sum_3"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "operator_group",
        "last_unit_price",
        "unit_price_change",
        "rows_in_warehouse_id",
        "distinct_operator_group_in_warehouse_id",
        "moved_at_percent_rank",
        "moved_at_percent_rank_2",
        "qty_change",
        "rows_in_sku_id",
        "second_running_unit_price_2",
    )
    .orderBy("rows_in_warehouse_id", F.col("moved_at_percent_rank_2").desc())
)
