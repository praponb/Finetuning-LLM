from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_distinct_operators_by_unit_p = Window.partitionBy("distinct_operators").orderBy(F.col("unit_price_operator_op_ann").desc(), "operator")
w_operator_distinct_operators = Window.partitionBy("operator", "distinct_operators")
w_distinct_operators_by_total = Window.partitionBy("distinct_operators").orderBy("total_days_since_moved_at", "operator").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_operators_by_total_2 = Window.partitionBy("distinct_operators").orderBy("total_days_since_moved_at", "operator").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_distinct_operators_2 = Window.partitionBy("operator", "distinct_operators").orderBy(F.col("operator_sum_total_days_since_moved_at").desc_nulls_first(), "operator")

scoped = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.concat_ws("|", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.days_since_moved_at",
        F.col("w.city").alias("warehouse_city"),
    )
)

grouped = (
    joined
    .groupBy("operator")
    .agg(
        F.sum(F.when(F.col("operator") == "op_ann", F.col("unit_price")).otherwise(0)).alias("unit_price_operator_op_ann"),
        F.sum("days_since_moved_at").alias("total_days_since_moved_at"),
        F.array_sort(F.collect_set("warehouse_city")).alias("warehouse_city_list"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.round(F.sum("unit_price") / F.nullif(F.sum("days_since_moved_at"), F.lit(0)), 3).alias("unit_price_to_days_since_moved_at_ratio"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
)

qualified = (
    grouped
    .crossJoin(grouped.agg(F.avg("total_days_since_moved_at").alias("overall_avg_total_days_since_moved_at")))
    .filter(F.col("total_days_since_moved_at") > F.col("overall_avg_total_days_since_moved_at"))
    .drop("overall_avg_total_days_since_moved_at")
)

with_windows = (
    qualified
    .select(
        "*",
        F.lead("unit_price_operator_op_ann").over(w_distinct_operators_by_unit_p).alias("next_unit_price_operator_op_ann"),
        F.sum("total_days_since_moved_at").over(w_operator).alias("operator_sum_total_days_since_moved_at"),
        F.round(F.avg("unit_price_operator_op_ann").over(w_operator), 2).alias("operator_avg_unit_price_operator_op_ann"),
        F.max("total_days_since_moved_at").over(w_operator).alias("operator_max_total_days_since_moved_at"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.round(F.col("total_days_since_moved_at") / F.nullif(F.sum("total_days_since_moved_at").over(w_operator_distinct_operators), F.lit(0)), 4).alias("total_days_since_moved_at_share_of_operator_dist"),
        F.nth_value("distinct_operators", 2).over(w_distinct_operators_by_total).alias("second_distinct_operators"),
        F.count(F.lit(1)).over(w_distinct_operators_by_total_2).alias("running_count"),
        F.lag("total_days_since_moved_at").over(w_operator_distinct_operators_2).alias("prev_total_days_since_moved_at"),
        F.last("distinct_operators", ignorenulls=True).over(w_distinct_operators_by_total_2).alias("last_known_distinct_operators"),
    )
)

with_activity = (
    with_windows_2
    .crossJoin(with_windows_2.agg(F.max("operator_sum_total_days_since_moved_at").alias("overall_max_operator_sum_total_days_since_moved_")))
    .filter(F.col("operator_sum_total_days_since_moved_at") < F.col("overall_max_operator_sum_total_days_since_moved_"))
    .drop("overall_max_operator_sum_total_days_since_moved_")
)

result = (
    with_activity
    .filter(F.col("last_known_distinct_operators") >= 2)
    .select(
        "operator",
        "unit_price_operator_op_ann",
        "total_days_since_moved_at",
        "warehouse_city_list",
        "distinct_operators",
        "unit_price_to_days_since_moved_at_ratio",
        "operator_list",
        "operator_avg_unit_price_operator_op_ann",
        "total_days_since_moved_at_share_of_operator_dist",
        "running_count",
        "prev_total_days_since_moved_at",
        "last_known_distinct_operators",
    )
    .orderBy(F.col("prev_total_days_since_moved_at").desc_nulls_first())
)
