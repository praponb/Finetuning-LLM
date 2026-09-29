from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_sku_u = Window.partitionBy("warehouse_id", "sku_id").orderBy("sku_unit_cost")
w_all_by_qty_moving_2 = Window.orderBy("qty", "id").rowsBetween(-2, Window.currentRow)
w_operator_move_type_by_unit_p = Window.partitionBy("operator", "move_type").orderBy("unit_price")
w_warehouse_id_sku_id_by_sku_u_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy("sku_unit_cost", "id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_operator_by_sku_unit = Window.partitionBy("sku_id", "operator").orderBy(F.col("sku_unit_cost").desc(), "id")
w_all_by_qty = Window.orderBy("qty", "id")
w_sku_id_operator_by_sku_unit_2 = Window.partitionBy("sku_id", "operator").orderBy(F.col("sku_unit_cost").desc())
w_all_by_qty_ties = Window.orderBy("qty")
w_warehouse_id_by_qty_rank_2_r = Window.partitionBy("warehouse_id").orderBy(F.col("qty_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_qty_rank_2_by_q = Window.partitionBy("warehouse_id", "qty_rank_2").orderBy(F.col("qty_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_unit_price_dense_rank_operat = Window.partitionBy("unit_price_dense_rank", "operator")
w_distinct_warehouse_id_in_sku = Window.partitionBy("distinct_warehouse_id_in_sku_id_operator", "operator").orderBy("next_qty", "id")

recent_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
        (F.col("moved_at") + F.expr("INTERVAL 1 DAYS")).alias("moved_at_plus_1d"),
    )
)

with_details = (
    recent_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "sku_unit_cost",
        F.round(F.percent_rank().over(w_warehouse_id_sku_id_by_sku_u), 3).alias("sku_unit_cost_percent_rank"),
        F.sum("sku_unit_cost").over(w_all_by_qty_moving_2).alias("sku_unit_cost_sum_3"),
        F.dense_rank().over(w_operator_move_type_by_unit_p).alias("unit_price_dense_rank"),
        (F.col("unit_price") - F.lag("unit_price").over(w_warehouse_id_sku_id_by_sku_u_2)).alias("unit_price_change"),
        F.count(F.lit(1)).over(w_sku_id_operator).alias("rows_in_sku_id_operator"),
        F.round(F.col("sku_unit_cost") / F.nullif(F.sum("sku_unit_cost").over(w_warehouse_id), F.lit(0)), 4).alias("sku_unit_cost_share_of_warehouse_id"),
        F.ntile(5).over(w_sku_id_operator_by_sku_unit).alias("sku_unit_cost_quintile"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_operator), F.lit(0)), 4).alias("unit_price_share_of_sku_id_operator"),
        F.lead("qty", 1, 0).over(w_sku_id_operator_by_sku_unit).alias("next_qty"),
        F.rank().over(w_all_by_qty).alias("qty_rank"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id_operator)).alias("distinct_warehouse_id_in_sku_id_operator"),
        F.round(F.cume_dist().over(w_sku_id_operator_by_sku_unit_2), 3).alias("sku_unit_cost_cume_dist"),
        F.rank().over(w_all_by_qty_ties).alias("qty_rank_2"),
        F.ntile(4).over(w_all_by_qty).alias("qty_quartile"),
    )
)

leaders = metrics.filter((F.col("unit_price_dense_rank") == 1) & (F.col("warehouse_id").isNotNull()))

with_trends = (
    leaders
    .select(
        "*",
        F.max("qty_quartile").over(w_warehouse_id_by_qty_rank_2_r).alias("max_qty_quartile_so_far"),
        F.nth_value("unit_price", 2).over(w_warehouse_id_qty_rank_2_by_q).alias("second_unit_price"),
        F.round(F.col("qty_quartile") / F.nullif(F.sum("qty_quartile").over(w_unit_price_dense_rank_operat), F.lit(0)), 4).alias("qty_quartile_share_of_unit_price_dense_rank_oper"),
        F.ntile(5).over(w_distinct_warehouse_id_in_sku).alias("next_qty_quintile"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "qty",
        "operator",
        "unit_price",
        "sku_unit_cost",
        "sku_unit_cost_percent_rank",
        "sku_unit_cost_sum_3",
        "unit_price_dense_rank",
        "unit_price_change",
        "rows_in_sku_id_operator",
        "unit_price_share_of_sku_id_operator",
        "next_qty",
        "distinct_warehouse_id_in_sku_id_operator",
        "sku_unit_cost_cume_dist",
        "qty_rank_2",
        "qty_quartile",
        "max_qty_quartile_so_far",
        "second_unit_price",
        "next_qty_quintile",
    )
    .orderBy(F.col("unit_price_share_of_sku_id_operator").desc_nulls_first())
)
