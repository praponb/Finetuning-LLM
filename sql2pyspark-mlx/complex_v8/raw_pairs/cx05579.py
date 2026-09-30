from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_move_type_by_moved = Window.partitionBy("operator", "move_type").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_operator_move_type_by_moved_2 = Window.partitionBy("operator", "move_type").orderBy(F.col("moved_at").asc_nulls_last())
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_warehouse_id_by_unit_price_a = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price_adj").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_move_type_in_wareho = Window.partitionBy("distinct_move_type_in_warehouse_id_operator", "operator").orderBy(F.col("rows_in_move_type_operator").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_move_type_in_wareho_2 = Window.partitionBy("distinct_move_type_in_warehouse_id_operator", "operator").orderBy(F.col("rows_in_move_type_operator").desc(), "id")

base = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

ranked = (
    base
    .select(
        "id",
        "qty",
        "moved_at",
        "operator",
        "unit_price_adj",
        F.last("moved_at", ignorenulls=True).over(w_operator_move_type_by_moved).alias("last_known_moved_at"),
        F.max("qty").over(w_warehouse_id_operator).alias("warehouse_id_operator_max_qty"),
        F.dense_rank().over(w_operator_move_type_by_moved_2).alias("moved_at_dense_rank"),
        F.count(F.lit(1)).over(w_warehouse_id_operator).alias("rows_in_warehouse_id_operator"),
        F.count(F.lit(1)).over(w_move_type_operator).alias("rows_in_move_type_operator"),
        F.min("unit_price").over(w_move_type_operator).alias("move_type_operator_min_unit_price"),
        F.min("unit_price").over(w_warehouse_id_by_unit_price_a).alias("min_unit_price_so_far"),
        F.round(F.col("unit_price_adj") / F.nullif(F.sum("unit_price_adj").over(w_move_type_operator), F.lit(0)), 4).alias("unit_price_adj_share_of_move_type_operator"),
        F.size(F.collect_set("move_type").over(w_warehouse_id_operator)).alias("distinct_move_type_in_warehouse_id_operator"),
        F.round(F.col("unit_price_adj") / F.nullif(F.sum("unit_price_adj").over(w_warehouse_id_operator), F.lit(0)), 4).alias("unit_price_adj_share_of_warehouse_id_operator"),
        F.round(F.col("unit_price_adj") - F.avg("unit_price_adj").over(w_move_type_operator), 2).alias("unit_price_adj_vs_move_type_operator_avg"),
    )
)

enriched = (
    ranked
    .select(
        "id",
        "moved_at",
        "operator",
        "last_known_moved_at",
        "rows_in_warehouse_id_operator",
        "rows_in_move_type_operator",
        "move_type_operator_min_unit_price",
        "min_unit_price_so_far",
        "distinct_move_type_in_warehouse_id_operator",
        F.round((F.unix_timestamp(F.col("last_known_moved_at")) - F.unix_timestamp(F.col("moved_at"))) / 60, 1).alias("minutes_moved_at_to_last_known_moved_at"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.round(F.col("unit_price_adj") / F.nullif(F.col("moved_at_dense_rank"), F.lit(0)), 3).alias("unit_price_adj_per_moved_at_dense_rank"),
    )
)

ranked_2 = (
    enriched
    .select(
        "id",
        "moved_at",
        "operator",
        "last_known_moved_at",
        "rows_in_warehouse_id_operator",
        "rows_in_move_type_operator",
        "move_type_operator_min_unit_price",
        "min_unit_price_so_far",
        "distinct_move_type_in_warehouse_id_operator",
        "qty_bucket",
        "unit_price_adj_per_moved_at_dense_rank",
        F.nth_value("last_known_moved_at", 2).over(w_distinct_move_type_in_wareho).alias("second_last_known_moved_at"),
        F.rank().over(w_distinct_move_type_in_wareho_2).alias("rows_in_move_type_operator_rank"),
        (F.col("rows_in_move_type_operator") - F.lag("rows_in_move_type_operator").over(w_distinct_move_type_in_wareho_2)).alias("rows_in_move_type_operator_change"),
    )
)

result = (
    ranked_2
    .filter(F.col("move_type_operator_min_unit_price").between(1250.8, 4000.2))
    .select(
        "id",
        "operator",
        "last_known_moved_at",
        "rows_in_warehouse_id_operator",
        "rows_in_move_type_operator",
        "distinct_move_type_in_warehouse_id_operator",
        "qty_bucket",
        "unit_price_adj_per_moved_at_dense_rank",
        "second_last_known_moved_at",
        "rows_in_move_type_operator_rank",
    )
    .orderBy(F.col("qty_bucket").desc_nulls_first())
)
