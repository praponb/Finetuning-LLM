from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_least_qty_unit = Window.orderBy("total_least_qty_unit_price")
w_move_type_len = Window.partitionBy("move_type_len")
w_n_operator_in_op_bee_op_dew = Window.partitionBy("n_operator_in_op_bee_op_dew_op").orderBy("total_least_qty_unit_price", "move_type", "move_type_len", "warehouse_id")
w_move_type_by_n_operator_in_o = Window.partitionBy("move_type").orderBy("n_operator_in_op_bee_op_dew_op", "move_type", "move_type_len", "warehouse_id").rowsBetween(-2, Window.currentRow)

valid = (
    df_stock_moves
    .filter((F.col("unit_price").between(136.0, 307.8)) & (~F.col("move_type").isin("INBOUND", "ADJUST")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.when(F.col("move_type").isin("INBOUND", "OUTBOUND"), "group_a").when(F.col("move_type") == "ADJUST", "group_b").otherwise("other").alias("move_type_group"),
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "move_type_group",
        "least_qty_unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

summary = (
    enriched
    .groupBy("move_type", "move_type_len", "warehouse_id")
    .agg(
        F.sum("least_qty_unit_price").alias("total_least_qty_unit_price"),
        F.count(F.when(F.col("operator").isin("op_bee", "op_dew", "op_chan"), 1)).alias("n_operator_in_op_bee_op_dew_op"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.percentile_approx("qty", 0.5).alias("median_qty"),
    )
)

with_windows = (
    summary
    .select(
        "move_type",
        "move_type_len",
        "warehouse_id",
        "total_least_qty_unit_price",
        "operator_list",
        "median_qty",
        F.dense_rank().over(w_all_by_total_least_qty_unit).alias("total_least_qty_unit_price_dense_rank"),
        F.round(F.avg("median_qty").over(w_move_type_len), 2).alias("move_type_len_avg_median_qty"),
        F.lag("move_type_len").over(w_n_operator_in_op_bee_op_dew).alias("prev_move_type_len"),
        (F.col("total_least_qty_unit_price") - F.lag("total_least_qty_unit_price").over(w_n_operator_in_op_bee_op_dew)).alias("total_least_qty_unit_price_change"),
        F.ntile(4).over(w_n_operator_in_op_bee_op_dew).alias("total_least_qty_unit_price_quartile"),
        F.sum("move_type_len").over(w_move_type_by_n_operator_in_o).alias("move_type_len_sum_3"),
    )
)

result = (
    with_windows
    .filter(F.col("move_type_len_sum_3") <= 36)
    .select(
        "move_type",
        "move_type_len",
        "total_least_qty_unit_price",
        "operator_list",
        "median_qty",
        "total_least_qty_unit_price_dense_rank",
        "move_type_len_avg_median_qty",
        "prev_move_type_len",
        "total_least_qty_unit_price_change",
        "total_least_qty_unit_price_quartile",
        "move_type_len_sum_3",
    )
    .orderBy(F.col("prev_move_type_len").desc(), F.col("median_qty").asc())
)
