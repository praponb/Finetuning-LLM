from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_move_type_by_qty_ties = Window.partitionBy("sku_id", "move_type").orderBy(F.col("qty").asc_nulls_last())
w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_move_type = Window.partitionBy("move_type")
w_all_by_qty = Window.orderBy("qty", "id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_unit_price_share_of_m = Window.orderBy("unit_price_share_of_move_type_warehouse_id", "id")
w_distinct_operator_in_move_ty = Window.partitionBy("distinct_operator_in_move_type_warehouse_id", "move_type")
w_unit_price_dense_rank_by_uni = Window.partitionBy("unit_price_dense_rank").orderBy(F.col("unit_price_share_of_move_type_warehouse_id").desc(), "id")

scoped = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.round(F.cume_dist().over(w_sku_id_move_type_by_qty_ties), 3).alias("qty_cume_dist"),
        F.dense_rank().over(w_sku_id_move_type_by_qty_ties).alias("qty_dense_rank"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_move_type_warehouse_id"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type_warehouse_id), 2).alias("unit_price_vs_move_type_warehouse_id_avg"),
        F.min("unit_price").over(w_move_type).alias("move_type_min_unit_price"),
        F.lag("qty").over(w_all_by_qty).alias("prev_qty"),
        F.size(F.collect_set("operator").over(w_move_type_warehouse_id)).alias("distinct_operator_in_move_type_warehouse_id"),
        F.dense_rank().over(w_all_by_unit_price).alias("unit_price_dense_rank"),
        F.lag("moved_at").over(w_all_by_unit_price).alias("prev_moved_at"),
    )
)

top_per_group = (
    metrics
    .filter((F.col("qty_dense_rank") <= 5) & (F.col("move_type").isin("OUTBOUND", "ADJUST")))
)

with_trends = (
    top_per_group
    .select(
        "*",
        F.ntile(4).over(w_all_by_unit_price_share_of_m).alias("unit_price_share_of_move_type_warehouse_id_quart"),
        F.count(F.lit(1)).over(w_distinct_operator_in_move_ty).alias("rows_in_distinct_operator_in_move_type_warehouse"),
        F.sum("prev_qty").over(w_distinct_operator_in_move_ty).alias("distinct_operator_in_move_type_warehouse_id_move"),
        (F.col("prev_qty") - F.lag("prev_qty").over(w_unit_price_dense_rank_by_uni)).alias("prev_qty_change"),
        F.size(F.collect_set("operator").over(w_distinct_operator_in_move_ty)).alias("distinct_operator_in_distinct_operator_in_move_t"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "qty_cume_dist",
        "qty_dense_rank",
        "unit_price_share_of_move_type_warehouse_id",
        "unit_price_vs_move_type_warehouse_id_avg",
        "move_type_min_unit_price",
        "prev_qty",
        "distinct_operator_in_move_type_warehouse_id",
        "unit_price_dense_rank",
        "prev_moved_at",
        "rows_in_distinct_operator_in_move_type_warehouse",
        "distinct_operator_in_move_type_warehouse_id_move",
        "prev_qty_change",
        "distinct_operator_in_distinct_operator_in_move_t",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
    )
    .orderBy(
        F.col("distinct_operator_in_move_type_warehouse_id_move").desc(),
        F.col("unit_price_vs_move_type_warehouse_id_avg").desc(),
    )
)
