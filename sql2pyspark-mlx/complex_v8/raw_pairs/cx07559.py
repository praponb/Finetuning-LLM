from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_warehouse_id_operator_by_mov = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator = Window.partitionBy("operator")
w_warehouse_id_operator_by_mov_2 = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("moved_at").asc_nulls_last())
w_move_type_by_warehouse_capac = Window.partitionBy("move_type").orderBy(F.col("warehouse_capacity").desc())
w_operator_move_type_by_wareho = Window.partitionBy("operator", "move_type").orderBy("warehouse_capacity", "id")
w_sku_id_operator_by_greatest = Window.partitionBy("sku_id", "operator").orderBy(F.col("greatest_qty_unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_warehouse_capac_2 = Window.partitionBy("move_type").orderBy(F.col("warehouse_capacity").desc(), "id")

base = (
    df_stock_moves
    .filter((F.col("unit_price") < 313.8) & (F.col("qty") >= 109))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.greatest(F.col("qty"), F.col("unit_price")).alias("greatest_qty_unit_price"),
    )
)

enriched = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.city").isin("Kuala Lumpur", "Singapore"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.greatest_qty_unit_price",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "warehouse_capacity",
        F.count(F.lit(1)).over(w_warehouse_id_sku_id).alias("rows_in_warehouse_id_sku_id"),
        F.first("greatest_qty_unit_price", ignorenulls=True).over(w_warehouse_id_operator_by_mov).alias("first_known_greatest_qty_unit_price"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_operator), 2).alias("warehouse_capacity_vs_operator_avg"),
        F.sum("greatest_qty_unit_price").over(w_operator).alias("operator_sum_greatest_qty_unit_price"),
        F.size(F.collect_set("sku_id").over(w_operator)).alias("distinct_sku_id_in_operator"),
        F.round(F.col("greatest_qty_unit_price") / F.nullif(F.sum("greatest_qty_unit_price").over(w_warehouse_id_sku_id), F.lit(0)), 4).alias("greatest_qty_unit_price_share_of_warehouse_id_sk"),
        F.round(F.cume_dist().over(w_warehouse_id_operator_by_mov_2), 3).alias("moved_at_cume_dist"),
        F.round(F.percent_rank().over(w_move_type_by_warehouse_capac), 3).alias("warehouse_capacity_percent_rank"),
        F.row_number().over(w_operator_move_type_by_wareho).alias("seq_no"),
        F.nth_value("moved_at", 2).over(w_sku_id_operator_by_greatest).alias("second_moved_at"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_move_type_by_warehouse_capac_2))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

first_rows = (
    ranked
    .filter(F.col("seq_no") <= 2)
    .drop("seq_no")
)

result = (
    first_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "warehouse_capacity",
        "rows_in_warehouse_id_sku_id",
        "first_known_greatest_qty_unit_price",
        "warehouse_capacity_vs_operator_avg",
        "operator_sum_greatest_qty_unit_price",
        "distinct_sku_id_in_operator",
        "greatest_qty_unit_price_share_of_warehouse_id_sk",
        "warehouse_capacity_percent_rank",
        "second_moved_at",
        "minutes_since_prev_moved_at",
    )
    .orderBy(
        "operator_sum_greatest_qty_unit_price",
        F.col("warehouse_capacity_percent_rank").desc(),
    )
)
