from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_operator_count = Window.partitionBy("move_operator_count")
w_move_operator_count_by_move = Window.partitionBy("move_operator_count").orderBy("move_operator_count_length_trim_wh_code_7", "move_operator_count").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = df_warehouses.filter(~F.col("city").isin("Bangkok", "Kuala Lumpur"))

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type") == "ADJUST", 1)).alias("move_move_type_count"),
        F.count(F.when(F.col("operator").isin("op_dew", "op_ann"), 1)).alias("move_operator_count"),
        F.count("*").alias("move_count"),
    )
)

profiled = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

totals = (
    profiled
    .filter(F.col("capacity") >= 4912)
    .groupBy("move_operator_count")
    .agg(
        F.round(F.sum("move_operator_count") / F.nullif(F.sum("move_count"), F.lit(0)), 3).alias("move_operator_count_to_move_count_ratio"),
        F.sum(F.when(F.length(F.trim(F.col("wh_code"))) >= 7, F.col("move_operator_count"))).alias("move_operator_count_length_trim_wh_code_7"),
    )
)

qualified = (
    totals
    .crossJoin(totals.agg(F.max("move_operator_count_length_trim_wh_code_7").alias("overall_max_move_operator_count_length_trim_wh_c")))
    .filter(F.col("move_operator_count_length_trim_wh_code_7") < F.col("overall_max_move_operator_count_length_trim_wh_c"))
    .drop("overall_max_move_operator_count_length_trim_wh_c")
)

with_windows = (
    qualified
    .select(
        "*",
        F.max("move_operator_count").over(w_move_operator_count).alias("move_operator_count_max_move_operator_count"),
        F.nth_value("move_operator_count", 2).over(w_move_operator_count_by_move).alias("second_move_operator_count"),
        F.round(F.col("move_operator_count") / F.nullif(F.sum("move_operator_count").over(w_move_operator_count), F.lit(0)), 4).alias("move_operator_count_share_of_move_operator_count"),
    )
)

result = (
    with_windows
    .filter(F.col("move_operator_count").between(2, 7))
    .select(
        "move_operator_count",
        "move_operator_count_to_move_count_ratio",
        "move_operator_count_length_trim_wh_code_7",
        "move_operator_count_max_move_operator_count",
        "second_move_operator_count",
    )
    .orderBy(F.col("move_operator_count_length_trim_wh_code_7").desc_nulls_first())
)
