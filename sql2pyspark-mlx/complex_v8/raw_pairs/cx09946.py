from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_pct_move_operator_not = Window.orderBy(F.col("pct_move_operator_not_in_op_eve").desc())
w_row_count = Window.partitionBy("row_count")
w_all_by_pct_move_operator_not_2 = Window.orderBy(F.col("pct_move_operator_not_in_op_eve").desc(), "operator")
w_pct_move_operator_not_in_op = Window.partitionBy("pct_move_operator_not_in_op_eve_tercile").orderBy("row_count_sum_row_count", "operator").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_row_count_pct_move_operator = Window.partitionBy("row_count", "pct_move_operator_not_in_op_eve_tercile")
w_pct_move_operator_not_in_op_2 = Window.partitionBy("pct_move_operator_not_in_op_eve_tercile").orderBy("row_count_sum_row_count")
w_pct_move_operator_not_in_op_3 = Window.partitionBy("pct_move_operator_not_in_op_eve_tercile").orderBy("row_count_sum_row_count", "operator")
w_row_count_sum_row_count_terc = Window.partitionBy("row_count_sum_row_count_tercile").orderBy("row_count_sum_row_count_percent_rank", "operator")
w_row_count_sum_row_count_rank = Window.partitionBy("row_count_sum_row_count_rank", "operator").orderBy(F.col("row_count_sum_row_count_percent_rank").desc())

cleaned = (
    df_stock_moves
    .filter((F.col("unit_price") < 398.5) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
    )
)

exploded_detail = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.hour(F.col("st.moved_at")).between(10, 15)), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.operator_or_unknown",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
    )
)

rollup_base = (
    exploded_detail
    .groupBy("operator")
    .agg(
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(~F.col("move_operator").isin("op_eve", "op_chan"), 1)) / F.count("*"), 1).alias("pct_move_operator_not_in_op_eve"),
        F.avg("qty").alias("avg_qty_check"),
    )
    .filter(F.col("avg_qty_check") > 123)
    .drop("avg_qty_check")
)

windowed = (
    rollup_base
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_pct_move_operator_not), 3).alias("pct_move_operator_not_in_op_eve_percent_rank"),
        F.sum("row_count").over(w_row_count).alias("row_count_sum_row_count"),
        F.ntile(3).over(w_all_by_pct_move_operator_not_2).alias("pct_move_operator_not_in_op_eve_tercile"),
    )
)

metrics = (
    windowed
    .select(
        "operator",
        "row_count",
        "pct_move_operator_not_in_op_eve_percent_rank",
        F.first("row_count_sum_row_count").over(w_pct_move_operator_not_in_op).alias("first_row_count_sum_row_count"),
        F.round(F.col("row_count_sum_row_count") / F.nullif(F.sum("row_count_sum_row_count").over(w_row_count_pct_move_operator), F.lit(0)), 4).alias("row_count_sum_row_count_share_of_row_count_pct_m"),
        F.round(F.percent_rank().over(w_pct_move_operator_not_in_op_2), 3).alias("row_count_sum_row_count_percent_rank"),
        F.count(F.lit(1)).over(w_row_count_pct_move_operator).alias("rows_in_row_count_pct_move_operator_not_in_op_ev"),
        F.ntile(3).over(w_pct_move_operator_not_in_op_3).alias("row_count_sum_row_count_tercile"),
        F.rank().over(w_pct_move_operator_not_in_op_2).alias("row_count_sum_row_count_rank"),
    )
)

ranked = (
    metrics
    .select(
        "operator",
        "row_count",
        "pct_move_operator_not_in_op_eve_percent_rank",
        "row_count_sum_row_count_share_of_row_count_pct_m",
        "row_count_sum_row_count_percent_rank",
        "rows_in_row_count_pct_move_operator_not_in_op_ev",
        "row_count_sum_row_count_tercile",
        "row_count_sum_row_count_rank",
        F.row_number().over(w_row_count_sum_row_count_terc).alias("row_count_sum_row_count_percent_rank_seq"),
        F.round(F.percent_rank().over(w_row_count_sum_row_count_rank), 3).alias("row_count_sum_row_count_percent_rank_percent_ran"),
    )
)

result = (
    ranked
    .filter(F.col("row_count_sum_row_count_percent_rank_seq") > 2)
    .select(
        "operator",
        "row_count",
        "row_count_sum_row_count_share_of_row_count_pct_m",
        "row_count_sum_row_count_percent_rank",
        "rows_in_row_count_pct_move_operator_not_in_op_ev",
        "row_count_sum_row_count_tercile",
        "row_count_sum_row_count_rank",
        "row_count_sum_row_count_percent_rank_seq",
        "row_count_sum_row_count_percent_rank_percent_ran",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
    .orderBy(
        F.col("operator_or_unknown").asc_nulls_last(),
        F.col("row_count_sum_row_count_rank").desc(),
    )
)
