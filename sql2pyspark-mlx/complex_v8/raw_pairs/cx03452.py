from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_move = Window.partitionBy("warehouse_id", "sku_id").orderBy("move_type_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_moved_at_ties = Window.partitionBy("operator").orderBy("moved_at")
w_move_type_len_moved_at_to_da = Window.partitionBy("move_type_len", F.to_date("moved_at"))
w_move_type_len_moved_at_to_da_2 = Window.partitionBy("move_type_len", F.to_date("moved_at")).orderBy("move_type_len")
w_sku_id_by_move_type_len = Window.partitionBy("sku_id").orderBy("move_type_len", "id")
w_move_type_len = Window.partitionBy("move_type_len")
w_sku_id_by_move_type_len_ties = Window.partitionBy("sku_id").orderBy("move_type_len")
w_operator_by_moved_at = Window.partitionBy("operator").orderBy("moved_at", "id")
w_move_type_len_moved_at_to_da_3 = Window.partitionBy("move_type_len", F.to_date("moved_at")).orderBy("move_type_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_len_moved_at_to_da_4 = Window.partitionBy("move_type_len", F.to_date("moved_at")).orderBy("move_type_len", "id").rowsBetween(-1, Window.currentRow)
w_operator_operator_len_by_mov = Window.partitionBy("operator", "operator_len").orderBy("move_type_len", "id")
w_distinct_sku_id_in_move_type = Window.partitionBy("distinct_sku_id_in_move_type_len", "operator")
w_moved_at_rank_distinct_sku_i = Window.partitionBy("moved_at_rank", "distinct_sku_id_in_move_type_len_moved_at_to_dat").orderBy("move_type_len_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_moves = (
    df_stock_moves
    .filter((F.col("operator") == "op_ann") & (F.col("moved_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

ranked = (
    filtered_moves
    .select(
        "*",
        F.last("qty_per_unit_price").over(w_warehouse_id_sku_id_by_move).alias("last_qty_per_unit_price"),
        F.round(F.percent_rank().over(w_operator_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
        F.size(F.collect_set("sku_id").over(w_move_type_len_moved_at_to_da)).alias("distinct_sku_id_in_move_type_len_moved_at_to_dat"),
        F.rank().over(w_move_type_len_moved_at_to_da_2).alias("move_type_len_rank"),
        F.rank().over(w_sku_id_by_move_type_len).alias("move_type_len_rank_2"),
        F.count(F.lit(1)).over(w_move_type_len).alias("rows_in_move_type_len"),
        F.rank().over(w_operator_by_moved_at_ties).alias("moved_at_rank"),
        F.size(F.collect_set("sku_id").over(w_move_type_len)).alias("distinct_sku_id_in_move_type_len"),
        F.rank().over(w_sku_id_by_move_type_len_ties).alias("move_type_len_rank_3"),
        F.rank().over(w_operator_by_moved_at).alias("moved_at_rank_2"),
        F.size(F.collect_set("warehouse_id").over(w_move_type_len)).alias("distinct_warehouse_id_in_move_type_len"),
        F.max("qty_per_unit_price").over(w_move_type_len).alias("move_type_len_max_qty_per_unit_price"),
        F.first("moved_at").over(w_move_type_len_moved_at_to_da_3).alias("first_moved_at"),
        F.sum("qty").over(w_move_type_len_moved_at_to_da_4).alias("qty_sum_2"),
    )
)

derived = (
    ranked
    .filter(F.col("rows_in_move_type_len") <= 11)
    .select(
        "*",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.round((F.unix_timestamp(F.col("first_moved_at")) - F.unix_timestamp(F.col("moved_at"))) / 60, 1).alias("minutes_moved_at_to_first_moved_at"),
    )
)

ranked_2 = (
    derived
    .select(
        "id",
        "sku_id",
        "qty",
        "move_type_len",
        "qty_per_unit_price",
        "last_qty_per_unit_price",
        "moved_at_percent_rank",
        "distinct_sku_id_in_move_type_len_moved_at_to_dat",
        "move_type_len_rank",
        "move_type_len_rank_2",
        "rows_in_move_type_len",
        "moved_at_rank",
        "move_type_len_rank_3",
        "distinct_warehouse_id_in_move_type_len",
        "move_type_len_max_qty_per_unit_price",
        "qty_sum_2",
        "operator_len",
        "minutes_moved_at_to_first_moved_at",
        F.lag("move_type_len", 2).over(w_operator_operator_len_by_mov).alias("prev2_move_type_len"),
        F.round(F.col("moved_at_rank_2") - F.avg("moved_at_rank_2").over(w_distinct_sku_id_in_move_type), 2).alias("moved_at_rank_2_vs_distinct_sku_id_in_move_type_"),
        F.nth_value("move_type_len_rank_2", 2).over(w_moved_at_rank_distinct_sku_i).alias("second_move_type_len_rank_2"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "sku_id",
        "qty",
        "move_type_len",
        "qty_per_unit_price",
        "last_qty_per_unit_price",
        "moved_at_percent_rank",
        "distinct_sku_id_in_move_type_len_moved_at_to_dat",
        "move_type_len_rank",
        "move_type_len_rank_2",
        "rows_in_move_type_len",
        "moved_at_rank",
        "move_type_len_rank_3",
        "distinct_warehouse_id_in_move_type_len",
        "move_type_len_max_qty_per_unit_price",
        "qty_sum_2",
        "operator_len",
        "minutes_moved_at_to_first_moved_at",
        "prev2_move_type_len",
        "moved_at_rank_2_vs_distinct_sku_id_in_move_type_",
        "second_move_type_len_rank_2",
        F.greatest(F.col("move_type_len"), F.col("rows_in_move_type_len")).alias("greatest_move_type_len_rows_in_move_type_len"),
        (F.col("move_type_len_rank_2") + F.col("moved_at_percent_rank")).alias("move_type_len_rank_2_plus_moved_at_percent_rank"),
    )
    .orderBy(F.col("second_move_type_len_rank_2").asc_nulls_last(), "id")
    .limit(5)
)
