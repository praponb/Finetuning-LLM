from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_year_move_type_by_u = Window.partitionBy(F.year("moved_at"), "move_type").orderBy(F.col("unit_price").desc(), "id")
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_move_type_warehouse_id_by_mo = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("moved_at").desc(), "id")
w_moved_at_to_date = Window.partitionBy(F.to_date("moved_at"))
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_moved_at_year_move_type_by_u_2 = Window.partitionBy(F.year("moved_at"), "move_type").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_running = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy("moved_at", "id")

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_trends = (
    filtered
    .select(
        "id",
        "move_type",
        "operator",
        "unit_price",
        F.ntile(3).over(w_moved_at_year_move_type_by_u).alias("unit_price_tercile"),
        F.lag("moved_at", 2).over(w_all_by_moved_at).alias("prev2_moved_at"),
        F.lag("moved_at", 2).over(w_move_type_warehouse_id_by_mo).alias("prev2_moved_at_2"),
        F.size(F.collect_set("operator").over(w_moved_at_to_date)).alias("distinct_operator_in_moved_at_to_date"),
        F.count(F.lit(1)).over(w_moved_at_to_date).alias("rows_in_moved_at_to_date"),
        F.size(F.collect_set("move_type").over(w_moved_at_to_date)).alias("distinct_move_type_in_moved_at_to_date"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_operator), 2).alias("unit_price_vs_warehouse_id_operator_avg"),
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id_operator), F.lit(0)), 4).alias("unit_price_share_of_warehouse_id_operator"),
        F.max("unit_price").over(w_moved_at_year_move_type_by_u_2).alias("max_unit_price_so_far"),
        F.first("moved_at", ignorenulls=True).over(w_all_by_moved_at_running).alias("first_known_moved_at"),
        F.lead("moved_at").over(w_sku_id_move_type_by_moved_at).alias("next_moved_at"),
        F.max("unit_price").over(w_moved_at_to_date).alias("moved_at_to_date_max_unit_price"),
        F.min("unit_price").over(w_moved_at_to_date).alias("moved_at_to_date_min_unit_price"),
        F.rank().over(w_moved_at_year_move_type_by_u).alias("unit_price_rank"),
        F.min("unit_price").over(w_warehouse_id_operator).alias("warehouse_id_operator_min_unit_price"),
    )
)

top_rows = with_trends.filter(F.col("moved_at_dense_rank") <= 3)

result = (
    top_rows
    .select(
        "id",
        "operator",
        "unit_price",
        "unit_price_tercile",
        "prev2_moved_at",
        "prev2_moved_at_2",
        "distinct_operator_in_moved_at_to_date",
        "rows_in_moved_at_to_date",
        "distinct_move_type_in_moved_at_to_date",
        "unit_price_vs_warehouse_id_operator_avg",
        "moved_at_dense_rank",
        "max_unit_price_so_far",
        "first_known_moved_at",
        "next_moved_at",
        "moved_at_to_date_max_unit_price",
        "moved_at_to_date_min_unit_price",
        "unit_price_rank",
        "warehouse_id_operator_min_unit_price",
    )
    .orderBy("moved_at_to_date_max_unit_price")
)
