from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_sku_id_by_unit_pri = Window.partitionBy("move_type", "sku_id").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_operator_by_qty_minus_unit_p = Window.partitionBy("operator").orderBy("qty_minus_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_qty_minus_unit_p_2 = Window.partitionBy("operator").orderBy("qty_minus_unit_price", "id").rowsBetween(-1, -1)
w_move_type_by_qty_minus_unit = Window.partitionBy("move_type").orderBy(F.col("qty_minus_unit_price").desc(), "id")
w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_move_type_sku_id_by_unit_pri_2 = Window.partitionBy("move_type", "sku_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_unit_price = Window.partitionBy("move_type").orderBy("unit_price", "id")
w_move_type_by_qty_minus_unit_2 = Window.partitionBy("move_type").orderBy(F.col("qty_minus_unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_sku_id_by_unit_pri_3 = Window.partitionBy("move_type", "sku_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-2, -1)

scoped = (
    df_stock_moves
    .filter((F.col("move_type") != "TRANSFER") & (F.col("unit_price") > 215.4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.ntile(5).over(w_move_type_sku_id_by_unit_pri).alias("unit_price_quintile"),
        F.round(F.col("days_since_moved_at") - F.avg("days_since_moved_at").over(w_warehouse_id), 2).alias("days_since_moved_at_vs_warehouse_id_avg"),
        F.nth_value("days_since_moved_at", 2).over(w_operator_by_qty_minus_unit_p).alias("second_days_since_moved_at"),
        F.round(F.avg("days_since_moved_at").over(w_operator_by_qty_minus_unit_p_2), 2).alias("days_since_moved_at_avg_1_prior"),
        F.ntile(4).over(w_move_type_by_qty_minus_unit).alias("qty_minus_unit_price_quartile"),
        F.size(F.collect_set("sku_id").over(w_move_type_warehouse_id)).alias("distinct_sku_id_in_move_type_warehouse_id"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.lag("qty_minus_unit_price").over(w_sku_id_by_unit_price).alias("prev_qty_minus_unit_price"),
        F.ntile(4).over(w_move_type_sku_id_by_unit_pri).alias("unit_price_quartile"),
        F.round(F.col("qty_minus_unit_price") - F.avg("qty_minus_unit_price").over(w_warehouse_id), 2).alias("qty_minus_unit_price_vs_warehouse_id_avg"),
        F.last("unit_price").over(w_move_type_sku_id_by_unit_pri_2).alias("last_unit_price"),
        F.row_number().over(w_move_type_by_unit_price).alias("rn"),
        F.last("unit_price", ignorenulls=True).over(w_move_type_by_qty_minus_unit_2).alias("last_known_unit_price"),
        F.max("days_since_moved_at").over(w_move_type_sku_id_by_unit_pri_3).alias("days_since_moved_at_max_2_prior"),
        F.last("days_since_moved_at").over(w_move_type_sku_id_by_unit_pri_2).alias("last_days_since_moved_at"),
    )
)

top_per_group = (
    with_windows
    .filter(F.col("rn") == 1)
    .drop("rn")
)

rollup_base = (
    top_per_group
    .filter(F.col("days_since_moved_at_vs_warehouse_id_avg") < 490.0)
    .groupBy("unit_price_quintile", "operator")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("last_unit_price").alias("max_last_unit_price"),
        F.max("days_since_moved_at_max_2_prior").alias("max_days_since_moved_at_max_2_prior_check"),
    )
    .filter(F.col("max_days_since_moved_at_max_2_prior_check") > 210)
    .drop("max_days_since_moved_at_max_2_prior_check")
)

result = (
    rollup_base
    .select("unit_price_quintile", "max_last_unit_price")
    .orderBy("max_last_unit_price", F.col("unit_price_quintile").desc())
)
