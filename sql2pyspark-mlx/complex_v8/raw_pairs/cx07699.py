from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_to_date_operator = Window.partitionBy(F.to_date("moved_at"), "operator")
w_all_by_qty_ties = Window.orderBy("qty")
w_all_by_qty_full = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_all_by_qty_running = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price_full = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_sku_id_in_moved_at = Window.partitionBy("distinct_sku_id_in_moved_at_to_date_operator").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_unit_price_dense_rank_by_uni = Window.partitionBy("unit_price_dense_rank").orderBy("unit_price_dense_rank", "id")
w_distinct_sku_id_in_moved_at_2 = Window.partitionBy("distinct_sku_id_in_moved_at_to_date_operator").orderBy(F.col("moved_at").desc(), "id")
w_unit_price_dense_rank_distin = Window.partitionBy("unit_price_dense_rank", "distinct_sku_id_in_moved_at_to_date_operator")
w_n_rows_n_operator_in_op_chan = Window.partitionBy("n_rows", "n_operator_in_op_chan_op_bee_o")
w_qty_tier_by_moved_at_to_date = Window.partitionBy("qty_tier").orderBy(F.col("moved_at_to_date_operator_min_unit_price_move_ty").desc(), "qty_tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stock_moves
    .filter((F.col("operator") == "op_dew") | (F.col("unit_price") >= 86.3))
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

windowed = (
    base
    .select(
        "id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_moved_at_to_date_operator), F.lit(0)), 4).alias("qty_share_of_moved_at_to_date_operator"),
        F.size(F.collect_set("move_type").over(w_moved_at_to_date_operator)).alias("distinct_move_type_in_moved_at_to_date_operator"),
        F.size(F.collect_set("sku_id").over(w_moved_at_to_date_operator)).alias("distinct_sku_id_in_moved_at_to_date_operator"),
        F.round(F.percent_rank().over(w_all_by_qty_ties), 3).alias("qty_percent_rank"),
        F.first("unit_price").over(w_all_by_qty_full).alias("first_unit_price"),
        F.size(F.collect_set("warehouse_id").over(w_moved_at_to_date_operator)).alias("distinct_warehouse_id_in_moved_at_to_date_operat"),
        F.dense_rank().over(w_all_by_unit_price_ties).alias("unit_price_dense_rank"),
        F.count(F.lit(1)).over(w_moved_at_to_date_operator).alias("rows_in_moved_at_to_date_operator"),
        F.max("qty").over(w_all_by_qty_running).alias("max_qty_so_far"),
        F.min("qty").over(w_moved_at_to_date_operator).alias("moved_at_to_date_operator_min_qty"),
        F.min("unit_price").over(w_moved_at_to_date_operator).alias("moved_at_to_date_operator_min_unit_price"),
        F.last("unit_price").over(w_all_by_unit_price_full).alias("last_unit_price"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.sum("first_unit_price").over(w_distinct_sku_id_in_moved_at).alias("running_first_unit_price"),
        F.lag("first_unit_price", 1, 0).over(w_unit_price_dense_rank_by_uni).alias("prev_first_unit_price"),
        F.ntile(4).over(w_distinct_sku_id_in_moved_at_2).alias("moved_at_quartile"),
        F.sum("last_unit_price").over(w_unit_price_dense_rank_distin).alias("unit_price_dense_rank_distinct_sku_id_in_moved_a"),
        F.row_number().over(w_distinct_sku_id_in_moved_at_2).alias("seq_no"),
    )
)

totals = (
    windowed_2
    .groupBy(
        F.when(F.col("qty") >= 372, "large").when(F.col("qty") >= 199, "medium").otherwise("small").alias("qty_tier"),
    )
    .agg(
        F.count(F.when(F.col("operator").isin("op_chan", "op_bee", "op_eve"), 1)).alias("n_operator_in_op_chan_op_bee_o"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("move_type").isin("INBOUND", "TRANSFER"), F.col("moved_at_to_date_operator_min_unit_price"))).alias("moved_at_to_date_operator_min_unit_price_move_ty"),
    )
)

with_windows = (
    totals
    .select(
        "*",
        F.count(F.lit(1)).over(w_n_rows_n_operator_in_op_chan).alias("rows_in_n_rows_n_operator_in_op_chan_op_bee_o"),
        F.first("n_rows", ignorenulls=True).over(w_qty_tier_by_moved_at_to_date).alias("first_known_n_rows"),
    )
)

result = (
    with_windows
    .select(
        "qty_tier",
        "n_operator_in_op_chan_op_bee_o",
        "n_rows",
        "moved_at_to_date_operator_min_unit_price_move_ty",
        "first_known_n_rows",
    )
    .orderBy(F.col("n_rows").asc(), F.col("first_known_n_rows").asc())
)
