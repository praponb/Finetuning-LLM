from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_len_cnt = Window.partitionBy("move_type_len", "cnt")
w_all_by_cnt_moving_3 = Window.orderBy(F.col("cnt").desc_nulls_first(), "move_type_len").rowsBetween(-3, Window.currentRow)
w_all_by_cnt = Window.orderBy(F.col("cnt").desc_nulls_first(), "move_type_len")
w_n_operator_op_chan_by_move_t = Window.partitionBy("n_operator_op_chan").orderBy("move_type_len_share_of_move_type_len_cnt", "move_type_len").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_operator_op_chan_by_move_t_2 = Window.partitionBy("n_operator_op_chan").orderBy("move_type_len_share_of_move_type_len_cnt", "move_type_len")
w_move_type_len_by_next_total = Window.partitionBy("move_type_len").orderBy(F.col("next_total_unit_price").desc_nulls_first(), "move_type_len")
w_move_type_len_by_next_total_2 = Window.partitionBy("move_type_len").orderBy(F.col("next_total_unit_price").desc_nulls_first(), "move_type_len").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") >= 4) | (F.col("sku_id") <= 14))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

grouped = (
    prepared_moves
    .filter(F.col("move_type_prefix").isNotNull())
    .groupBy("move_type_len")
    .agg(
        F.sum(F.when(F.col("operator") == "op_chan", 1).otherwise(0)).alias("n_operator_op_chan"),
        F.count("*").alias("cnt"),
        F.sum("unit_price").alias("total_unit_price"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.sum("move_type_len").alias("sum_move_type_len_check"),
    )
    .filter(F.col("sum_move_type_len_check") > 7)
    .drop("sum_move_type_len_check")
)

without_matches = (
    grouped
    .crossJoin(grouped.agg(F.max("cnt").alias("overall_max_cnt")))
    .filter(F.col("cnt") < F.col("overall_max_cnt"))
    .drop("overall_max_cnt")
)

with_windows = (
    without_matches
    .select(
        "move_type_len",
        "n_operator_op_chan",
        "cnt",
        F.round(F.col("move_type_len") / F.nullif(F.sum("move_type_len").over(w_move_type_len_cnt), F.lit(0)), 4).alias("move_type_len_share_of_move_type_len_cnt"),
        F.sum("cnt").over(w_move_type_len_cnt).alias("move_type_len_cnt_sum_cnt"),
        F.max("total_unit_price").over(w_all_by_cnt_moving_3).alias("total_unit_price_max_4"),
        F.lead("total_unit_price").over(w_all_by_cnt).alias("next_total_unit_price"),
    )
)

with_trends = (
    with_windows
    .select(
        "move_type_len",
        "n_operator_op_chan",
        "cnt",
        "move_type_len_cnt_sum_cnt",
        "total_unit_price_max_4",
        "next_total_unit_price",
        F.first("next_total_unit_price", ignorenulls=True).over(w_n_operator_op_chan_by_move_t).alias("first_known_next_total_unit_price"),
        (F.col("n_operator_op_chan") - F.lag("n_operator_op_chan").over(w_n_operator_op_chan_by_move_t_2)).alias("n_operator_op_chan_change"),
        F.lead("move_type_len").over(w_n_operator_op_chan_by_move_t_2).alias("next_move_type_len"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        (F.col("cnt") - F.lag("cnt").over(w_move_type_len_by_next_total)).alias("cnt_change"),
        F.round(F.cume_dist().over(w_move_type_len_by_next_total), 3).alias("next_total_unit_price_cume_dist"),
        F.round(F.col("cnt") / F.nullif(F.sum("cnt").over(w_move_type_len_cnt), F.lit(0)), 4).alias("cnt_share_of_move_type_len_cnt"),
        F.first("total_unit_price_max_4").over(w_move_type_len_by_next_total_2).alias("first_total_unit_price_max_4"),
        F.row_number().over(w_move_type_len_by_next_total).alias("rn"),
    )
)

top_per_group = with_trends_2.filter(F.col("rn") <= 5)

result = (
    top_per_group
    .select(
        "move_type_len",
        "n_operator_op_chan",
        "cnt",
        "move_type_len_cnt_sum_cnt",
        "total_unit_price_max_4",
        "next_total_unit_price",
        "first_known_next_total_unit_price",
        "n_operator_op_chan_change",
        "next_move_type_len",
        "cnt_change",
        "next_total_unit_price_cume_dist",
        "cnt_share_of_move_type_len_cnt",
        "rn",
        F.greatest(F.col("next_move_type_len"), F.col("next_total_unit_price")).alias("greatest_next_move_type_len_next_total_unit_pric"),
    )
    .orderBy("n_operator_op_chan_change")
)
