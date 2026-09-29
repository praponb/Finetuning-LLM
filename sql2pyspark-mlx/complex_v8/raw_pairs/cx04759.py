from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator_by_uni = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("unit_price").desc())
w_all_by_moved_at_year_prior_3 = Window.orderBy(F.col("moved_at_year").desc(), "id").rowsBetween(-3, -1)
w_moved_at_year_by_moved_at_ti = Window.partitionBy("moved_at_year").orderBy("moved_at")
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_moved_at_year_by_moved_at = Window.partitionBy("moved_at_year").orderBy("moved_at", "id")
w_sku_id_by_qty = Window.partitionBy("sku_id").orderBy(F.col("qty").asc_nulls_last(), "id")
w_all_by_moved_at_year = Window.orderBy(F.col("moved_at_year").desc(), "id")
w_operator_move_type_by_qty = Window.partitionBy("operator", "move_type").orderBy(F.col("qty").asc_nulls_last(), "id")
w_qty_quintile_rn = Window.partitionBy("qty_quintile", "rn")
w_all_by_distinct_warehouse_id = Window.orderBy(F.col("distinct_warehouse_id_in_move_type_operator").desc(), "id")
w_all_by_distinct_warehouse_id_2 = Window.orderBy(F.col("distinct_warehouse_id_in_move_type_operator").desc(), "id").rowsBetween(-1, -1)
w_next_moved_at_year_distinct = Window.partitionBy(F.year("next_moved_at"), "distinct_warehouse_id_in_move_type_operator").orderBy(F.col("moved_at_rank").desc(), "id")
w_next_moved_at_year_distinct_2 = Window.partitionBy(F.year("next_moved_at"), "distinct_warehouse_id_in_move_type_operator").orderBy(F.col("moved_at_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next_moved_at_year_distinct_3 = Window.partitionBy(F.year("next_moved_at"), "distinct_warehouse_id_in_move_type_operator").orderBy(F.col("moved_at_rank").desc(), "id").rowsBetween(-3, Window.currentRow)

scoped_moves = (
    df_stock_moves
    .filter(F.col("sku_id") >= 12)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(": ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.year(F.col("moved_at")).alias("moved_at_year"),
    )
)

sequenced = (
    scoped_moves
    .select(
        "*",
        F.round(F.cume_dist().over(w_warehouse_id_operator_by_uni), 3).alias("unit_price_cume_dist"),
        F.max("unit_price").over(w_all_by_moved_at_year_prior_3).alias("unit_price_max_3_prior"),
        F.rank().over(w_moved_at_year_by_moved_at_ti).alias("moved_at_rank"),
        F.size(F.collect_set("sku_id").over(w_move_type_operator)).alias("distinct_sku_id_in_move_type_operator"),
        F.lead("moved_at").over(w_moved_at_year_by_moved_at).alias("next_moved_at"),
        F.ntile(5).over(w_sku_id_by_qty).alias("qty_quintile"),
        F.size(F.collect_set("warehouse_id").over(w_move_type_operator)).alias("distinct_warehouse_id_in_move_type_operator"),
        F.row_number().over(w_all_by_moved_at_year).alias("rn"),
        F.dense_rank().over(w_operator_move_type_by_qty).alias("qty_dense_rank"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.size(F.collect_set("move_type").over(w_qty_quintile_rn)).alias("distinct_move_type_in_qty_quintile_rn"),
        F.lead("unit_price_max_3_prior", 3).over(w_all_by_distinct_warehouse_id).alias("next3_unit_price_max_3_prior"),
        F.round(F.avg("moved_at_year").over(w_all_by_distinct_warehouse_id_2), 2).alias("moved_at_year_avg_1_prior"),
        F.row_number().over(w_next_moved_at_year_distinct).alias("rn_2"),
        F.first("qty").over(w_next_moved_at_year_distinct_2).alias("first_qty"),
        F.round(F.avg("qty_dense_rank").over(w_next_moved_at_year_distinct_3), 2).alias("qty_dense_rank_avg_4"),
    )
)

result = (
    with_trends
    .filter(F.col("distinct_move_type_in_qty_quintile_rn") > 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "operator_move_type_label",
        "distinct_sku_id_in_move_type_operator",
        "next_moved_at",
        "distinct_warehouse_id_in_move_type_operator",
        "qty_dense_rank",
        "distinct_move_type_in_qty_quintile_rn",
        "moved_at_year_avg_1_prior",
        "rn_2",
        "first_qty",
        "qty_dense_rank_avg_4",
        (F.floor(F.col("qty_dense_rank") / 2) * 2).alias("qty_dense_rank_bucket"),
        (F.col("moved_at") + F.expr("INTERVAL 2 HOURS")).alias("moved_at_plus_2h"),
    )
    .orderBy("qty_dense_rank_bucket", "rn_2", "id")
    .limit(20)
)
