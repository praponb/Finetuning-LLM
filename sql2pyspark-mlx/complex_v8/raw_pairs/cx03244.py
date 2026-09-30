from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_hour_by_moved_at_ho = Window.partitionBy("moved_at_hour").orderBy("moved_at_hour_to_unit_price_ratio", "operator", "moved_at_hour", "operator_len").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_hour_by_moved_at_ho_2 = Window.partitionBy("moved_at_hour").orderBy("moved_at_hour_to_unit_price_ratio")
w_all_by_moved_at_hour_to_unit = Window.orderBy(F.col("moved_at_hour_to_unit_price_ratio").desc(), "operator", "moved_at_hour", "operator_len").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_warehouse_ids = Window.partitionBy("distinct_warehouse_ids")

base_moves = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(2, 3)) & (~F.col("move_type").isin("OUTBOUND", "TRANSFER")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
    )
)

grouped = (
    base_moves
    .groupBy("operator", "moved_at_hour", "operator_len")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("operator").isin("op_bee", "op_dew"), 1)).alias("n_operator_in_op_bee_op_dew"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.round(F.sum("moved_at_hour") / F.nullif(F.sum("unit_price"), F.lit(0)), 3).alias("moved_at_hour_to_unit_price_ratio"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
    )
    .filter(F.col("cnt") >= 3)
)

with_windows = (
    grouped
    .select(
        "operator",
        "moved_at_hour",
        "operator_len",
        "n_operator_in_op_bee_op_dew",
        "distinct_move_types",
        "distinct_warehouse_ids",
        F.max("n_operator_in_op_bee_op_dew").over(w_moved_at_hour_by_moved_at_ho).alias("max_n_operator_in_op_bee_op_dew_so_far"),
        F.dense_rank().over(w_moved_at_hour_by_moved_at_ho_2).alias("moved_at_hour_to_unit_price_ratio_dense_rank"),
        F.first("operator_len", ignorenulls=True).over(w_moved_at_hour_by_moved_at_ho).alias("first_known_operator_len"),
        F.sum("n_operator_in_op_bee_op_dew").over(w_all_by_moved_at_hour_to_unit).alias("running_n_operator_in_op_bee_op_dew"),
        F.round(F.col("distinct_move_types") / F.nullif(F.sum("distinct_move_types").over(w_distinct_warehouse_ids), F.lit(0)), 4).alias("distinct_move_types_share_of_distinct_warehouse_"),
    )
)

result = (
    with_windows
    .cube("operator_len", "distinct_move_types", "moved_at_hour")
    .agg(
        F.sum("distinct_move_types").alias("total_distinct_move_types"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
    .orderBy(
        F.col("operator_len").asc_nulls_last(),
        F.col("distinct_move_types").asc_nulls_last(),
        F.col("moved_at_hour").asc_nulls_last(),
    )
)
