from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_move_type_by_moved = Window.partitionBy("operator", "move_type").orderBy("moved_at", "id")
w_operator_move_type = Window.partitionBy("operator", "move_type")
w_operator_n_rows = Window.partitionBy("operator", "n_rows")
w_move_type_by_n_rows = Window.partitionBy("move_type").orderBy("n_rows", "operator", "move_type")
w_rn_distinct_move_type_in_ope = Window.partitionBy("rn", "distinct_move_type_in_operator_n_rows").orderBy(F.col("total_moved_at_decile_change").desc(), "operator", "move_type")
w_n_rows = Window.partitionBy("n_rows")

prepared_moves = (
    df_stock_moves
    .filter((F.col("move_type").isin("OUTBOUND", "TRANSFER", "ADJUST")) & (F.col("moved_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

tagged = (
    prepared_moves
    .select(
        "id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

ranked = (
    tagged
    .select(
        "*",
        F.ntile(10).over(w_operator_move_type_by_moved).alias("moved_at_decile"),
        F.count(F.lit(1)).over(w_operator_move_type).alias("rows_in_operator_move_type"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_operator_move_type_by_moved))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

summary = (
    ranked
    .groupBy("operator", "move_type")
    .agg(
        F.round(F.sum("minutes_since_prev_moved_at") / F.nullif(F.sum("rows_in_operator_move_type"), F.lit(0)), 3).alias("minutes_since_prev_moved_at_to_rows_in_operator_"),
        F.sum("moved_at_decile").alias("total_moved_at_decile"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
)

with_windows = (
    summary
    .select(
        "operator",
        "move_type",
        "n_rows",
        "operator_list",
        F.max("total_moved_at_decile").over(w_operator_n_rows).alias("operator_n_rows_max_total_moved_at_decile"),
        F.max("n_rows").over(w_operator_n_rows).alias("operator_n_rows_max_n_rows"),
        (F.col("total_moved_at_decile") - F.lag("total_moved_at_decile").over(w_move_type_by_n_rows)).alias("total_moved_at_decile_change"),
        F.size(F.collect_set("move_type").over(w_operator_n_rows)).alias("distinct_move_type_in_operator_n_rows"),
        F.row_number().over(w_move_type_by_n_rows).alias("rn"),
        F.lag("total_moved_at_decile").over(w_move_type_by_n_rows).alias("prev_total_moved_at_decile"),
    )
)

qualified = (
    with_windows
    .crossJoin(with_windows.agg(F.max("rn").alias("overall_max_rn")))
    .filter(F.col("rn") < F.col("overall_max_rn"))
    .drop("overall_max_rn")
)

with_trends = (
    qualified
    .select(
        "*",
        F.row_number().over(w_rn_distinct_move_type_in_ope).alias("seq_no"),
        (F.col("rn") - F.lag("rn").over(w_rn_distinct_move_type_in_ope)).alias("rn_change"),
        F.sum("rn").over(w_n_rows).alias("n_rows_sum_rn"),
        F.size(F.collect_set("move_type").over(w_n_rows)).alias("distinct_move_type_in_n_rows"),
    )
)

result = (
    with_trends
    .select(
        "operator",
        "n_rows",
        "operator_list",
        "operator_n_rows_max_total_moved_at_decile",
        "operator_n_rows_max_n_rows",
        "total_moved_at_decile_change",
        "distinct_move_type_in_operator_n_rows",
        "rn",
        "seq_no",
        "rn_change",
        "n_rows_sum_rn",
        "distinct_move_type_in_n_rows",
        F.least(F.col("rn_change"), F.col("seq_no")).alias("least_rn_change_seq_no"),
        F.when(F.col("move_type").isin("INBOUND", "OUTBOUND"), "group_a").when(F.col("move_type") == "ADJUST", "group_b").otherwise("other").alias("move_type_group"),
    )
    .orderBy(
        F.col("operator_n_rows_max_n_rows").desc(),
        F.col("seq_no").desc_nulls_first(),
    )
)
