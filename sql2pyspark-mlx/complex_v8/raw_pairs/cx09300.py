from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_sku_id = Window.partitionBy("sku_id")
w_operator_by_moved_at = Window.partitionBy("operator").orderBy(F.col("moved_at").desc(), "id")
w_operator_sku_id_by_moved_at = Window.partitionBy("operator", "sku_id").orderBy("moved_at")
w_warehouse_id_by_moved_at_run = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_unit_price_runn = Window.partitionBy("move_type").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_sku_id_by_moved_at_2 = Window.partitionBy("operator", "sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_max_greatest_qty_unit = Window.orderBy("max_greatest_qty_unit_price_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_max_greatest_qty_unit_2 = Window.orderBy("max_greatest_qty_unit_price_so_far")
w_moved_at_rank_operator = Window.partitionBy("moved_at_rank", "operator")
w_all_by_sku_id_warehouse_id_m = Window.orderBy("sku_id_warehouse_id_min_greatest_qty_unit_price", "id").rowsBetween(-6, -1)
w_all_by_sku_id_warehouse_id_m_2 = Window.orderBy("sku_id_warehouse_id_min_greatest_qty_unit_price", "id")
w_all_by_max_greatest_qty_unit_3 = Window.orderBy("max_greatest_qty_unit_price_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.greatest(F.col("qty"), F.col("unit_price")).alias("greatest_qty_unit_price"),
        F.when(F.col("move_type").isin("ADJUST", "INBOUND"), "group_a").when(F.col("move_type") == "TRANSFER", "group_b").otherwise("other").alias("move_type_group"),
    )
)

windowed = (
    base
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "greatest_qty_unit_price",
        "move_type_group",
        F.min("greatest_qty_unit_price").over(w_sku_id_warehouse_id).alias("sku_id_warehouse_id_min_greatest_qty_unit_price"),
        F.round(F.col("greatest_qty_unit_price") - F.avg("greatest_qty_unit_price").over(w_sku_id), 2).alias("greatest_qty_unit_price_vs_sku_id_avg"),
        F.rank().over(w_operator_by_moved_at).alias("moved_at_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id), 2).alias("unit_price_vs_sku_id_avg"),
        F.round(F.cume_dist().over(w_operator_sku_id_by_moved_at), 3).alias("moved_at_cume_dist"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_sku_id_warehouse_id"),
        F.round(F.col("greatest_qty_unit_price") / F.nullif(F.sum("greatest_qty_unit_price").over(w_sku_id_warehouse_id), F.lit(0)), 4).alias("greatest_qty_unit_price_share_of_sku_id_warehous"),
        F.round(F.percent_rank().over(w_operator_by_moved_at), 3).alias("moved_at_percent_rank"),
        F.max("greatest_qty_unit_price").over(w_warehouse_id_by_moved_at_run).alias("max_greatest_qty_unit_price_so_far"),
        F.count(F.lit(1)).over(w_move_type_by_unit_price_runn).alias("running_count"),
        F.first("greatest_qty_unit_price", ignorenulls=True).over(w_operator_sku_id_by_moved_at_2).alias("first_known_greatest_qty_unit_price"),
    )
)

first_rows = windowed.filter(F.col("moved_at_rank") <= 3)

sequenced = (
    first_rows
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "greatest_qty_unit_price",
        "move_type_group",
        "sku_id_warehouse_id_min_greatest_qty_unit_price",
        "greatest_qty_unit_price_vs_sku_id_avg",
        "moved_at_rank",
        "unit_price_vs_sku_id_avg",
        "moved_at_cume_dist",
        "unit_price_share_of_sku_id_warehouse_id",
        "moved_at_percent_rank",
        "max_greatest_qty_unit_price_so_far",
        "first_known_greatest_qty_unit_price",
        F.last("moved_at_cume_dist", ignorenulls=True).over(w_all_by_max_greatest_qty_unit).alias("last_known_moved_at_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_max_greatest_qty_unit_2), 3).alias("max_greatest_qty_unit_price_so_far_cume_dist"),
        F.count(F.lit(1)).over(w_moved_at_rank_operator).alias("rows_in_moved_at_rank_operator"),
        F.round(F.avg("greatest_qty_unit_price").over(w_all_by_sku_id_warehouse_id_m), 2).alias("greatest_qty_unit_price_avg_6_prior"),
        (F.col("moved_at_rank") - F.lag("moved_at_rank").over(w_all_by_sku_id_warehouse_id_m_2)).alias("moved_at_rank_change"),
        F.first("unit_price_vs_sku_id_avg").over(w_all_by_max_greatest_qty_unit_3).alias("first_unit_price_vs_sku_id_avg"),
        F.round(F.col("moved_at_rank") / F.nullif(F.sum("moved_at_rank").over(w_moved_at_rank_operator), F.lit(0)), 4).alias("moved_at_rank_share_of_moved_at_rank_operator"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "greatest_qty_unit_price",
        "sku_id_warehouse_id_min_greatest_qty_unit_price",
        "greatest_qty_unit_price_vs_sku_id_avg",
        "moved_at_rank",
        "unit_price_vs_sku_id_avg",
        "moved_at_cume_dist",
        "unit_price_share_of_sku_id_warehouse_id",
        "moved_at_percent_rank",
        "max_greatest_qty_unit_price_so_far",
        "last_known_moved_at_cume_dist",
        "max_greatest_qty_unit_price_so_far_cume_dist",
        "rows_in_moved_at_rank_operator",
        "greatest_qty_unit_price_avg_6_prior",
        "first_unit_price_vs_sku_id_avg",
        F.substring(F.trim(F.col("move_type_group")), 1, 3).alias("move_type_group_prefix"),
    )
)
