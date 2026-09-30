from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_id_by_unit_pric = Window.partitionBy("operator", "sku_id").orderBy("unit_price", "id")
w_all_by_unit_price_adj_full = Window.orderBy("unit_price_adj", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator = Window.partitionBy("operator")
w_warehouse_id_by_unit_price_a = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price_adj").desc_nulls_first(), "id")
w_all_by_unit_price_adj_runnin = Window.orderBy("unit_price_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_warehouse_id_by_unit_price_a_2 = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price_adj").desc_nulls_first())
w_all_by_second_unit_price = Window.orderBy(F.col("second_unit_price").desc(), "id")
w_unit_price_adj_dense_rank_op = Window.partitionBy("unit_price_adj_dense_rank", "operator").orderBy("rows_in_warehouse_id_operator")
w_unit_price_adj_dense_rank_op_2 = Window.partitionBy("unit_price_adj_dense_rank", "operator").orderBy("rows_in_warehouse_id_operator", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_stock_moves
    .filter(F.col("sku_id") >= 4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
    )
)

windowed = (
    base
    .select(
        "*",
        F.lead("unit_price").over(w_operator_sku_id_by_unit_pric).alias("next_unit_price"),
        F.nth_value("unit_price", 2).over(w_all_by_unit_price_adj_full).alias("second_unit_price"),
        F.round(F.col("unit_price_adj") - F.avg("unit_price_adj").over(w_operator), 2).alias("unit_price_adj_vs_operator_avg"),
        (F.col("unit_price_adj") - F.lag("unit_price_adj").over(w_warehouse_id_by_unit_price_a)).alias("unit_price_adj_change"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
        F.count(F.lit(1)).over(w_all_by_unit_price_adj_runnin).alias("running_count"),
        F.count(F.lit(1)).over(w_warehouse_id_operator).alias("rows_in_warehouse_id_operator"),
        F.dense_rank().over(w_warehouse_id_by_unit_price_a_2).alias("unit_price_adj_dense_rank"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price",
        "move_type_operator_label",
        "next_unit_price",
        "second_unit_price",
        "unit_price_adj_vs_operator_avg",
        "rows_in_warehouse_id_operator",
        "unit_price_adj_dense_rank",
        F.row_number().over(w_all_by_second_unit_price).alias("seq_no"),
        F.rank().over(w_all_by_second_unit_price).alias("second_unit_price_rank"),
        F.round(F.cume_dist().over(w_unit_price_adj_dense_rank_op), 3).alias("rows_in_warehouse_id_operator_cume_dist"),
        F.first("unit_price_adj_change").over(w_unit_price_adj_dense_rank_op_2).alias("first_unit_price_adj_change"),
        F.lead("unit_price_adj_change", 3, 0).over(w_all_by_second_unit_price).alias("next3_unit_price_adj_change"),
        F.round(F.percent_rank().over(w_all_by_second_unit_price), 3).alias("second_unit_price_percent_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "sku_id",
        "unit_price",
        "move_type_operator_label",
        "second_unit_price",
        "rows_in_warehouse_id_operator",
        "unit_price_adj_dense_rank",
        "seq_no",
        "second_unit_price_rank",
        "rows_in_warehouse_id_operator_cume_dist",
        "next3_unit_price_adj_change",
        "second_unit_price_percent_rank",
    )
    .orderBy(F.col("id").asc())
    .limit(20)
)
