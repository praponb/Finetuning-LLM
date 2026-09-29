from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_last_moved_at_month = Window.partitionBy("operator", F.month("last_moved_at"))
w_all_by_distinct_move_types = Window.orderBy("distinct_move_types", "moved_at_day", "operator")
w_all_by_distinct_move_types_p = Window.orderBy("distinct_move_types", "moved_at_day", "operator").rowsBetween(-3, -1)

prepared_moves = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_chan", "op_eve"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean_2"),
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
    )
)

rollup_base = (
    prepared_moves
    .filter(F.col("unit_price") >= 119.5)
    .groupBy(F.to_date("moved_at").alias("moved_at_day"), "operator")
    .agg(
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("n_rows"),
    )
)

qualified = (
    rollup_base
    .crossJoin(rollup_base.agg(F.max("n_rows").alias("overall_max_n_rows")))
    .filter(F.col("n_rows") < F.col("overall_max_n_rows"))
    .drop("overall_max_n_rows")
)

qualified_2 = (
    qualified
    .crossJoin(qualified.agg(F.max("n_rows").alias("overall_max_n_rows")))
    .filter(F.col("n_rows") < F.col("overall_max_n_rows"))
    .drop("overall_max_n_rows")
)

windowed = (
    qualified_2
    .select(
        "*",
        F.sum("distinct_move_types").over(w_operator_last_moved_at_month).alias("operator_last_moved_at_month_sum_distinct_move_t"),
        F.lag("distinct_move_types", 2, 0).over(w_all_by_distinct_move_types).alias("prev2_distinct_move_types"),
        F.round(F.avg("n_rows").over(w_all_by_distinct_move_types_p), 2).alias("n_rows_avg_3_prior"),
    )
)

result = (
    windowed
    .select(
        "moved_at_day",
        "operator",
        "distinct_move_types",
        "move_type_list",
        "last_moved_at",
        "n_rows",
        "prev2_distinct_move_types",
        "n_rows_avg_3_prior",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at_day")).alias("days_since_moved_at_day"),
        F.abs(F.col("n_rows") - 7).alias("n_rows_distance"),
    )
    .orderBy(F.col("moved_at_day").asc(), "n_rows_avg_3_prior")
)
