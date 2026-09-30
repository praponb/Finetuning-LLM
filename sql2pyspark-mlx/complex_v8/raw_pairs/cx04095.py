from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_all_by_moved_at_prior_2 = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(-2, -1)
w_operator_move_type_by_unit_p = Window.partitionBy("operator", "move_type").orderBy(F.col("unit_price").desc())
w_all_by_unit_price_full = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type = Window.partitionBy("move_type")
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_all_by_unit_price_moving_2 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(-2, Window.currentRow)

valid = df_stock_moves.filter(F.col("sku_id") >= 3)

windowed = (
    valid
    .select(
        "*",
        F.size(F.collect_set("operator").over(w_sku_id)).alias("distinct_operator_in_sku_id"),
        F.max("unit_price").over(w_all_by_moved_at_prior_2).alias("unit_price_max_2_prior"),
        F.rank().over(w_operator_move_type_by_unit_p).alias("unit_price_rank"),
        F.first("unit_price").over(w_all_by_unit_price_full).alias("first_unit_price"),
        F.round(F.percent_rank().over(w_operator_move_type_by_unit_p), 3).alias("unit_price_percent_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type), 2).alias("unit_price_vs_move_type_avg"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_moved_at)).alias("unit_price_change"),
        F.round(F.avg("unit_price").over(w_all_by_unit_price_moving_2), 2).alias("unit_price_avg_3"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type), F.lit(0)), 4).alias("unit_price_share_of_move_type"),
    )
)

rollup_base = (
    windowed
    .groupBy("move_type", "distinct_operator_in_sku_id")
    .agg(
        F.round(F.sum("first_unit_price") / F.nullif(F.sum("unit_price_max_2_prior"), F.lit(0)), 3).alias("first_unit_price_to_unit_price_max_2_prior_ratio"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.round(F.sum("unit_price_percent_rank"), 2).alias("total_unit_price_percent_rank"),
        F.sum("unit_price_max_2_prior").alias("sum_unit_price_max_2_prior_check"),
    )
    .filter(F.col("sum_unit_price_max_2_prior_check") > 150.9)
    .drop("sum_unit_price_max_2_prior_check")
)

result = (
    rollup_base
    .filter(F.col("distinct_operator_in_sku_id") < 9)
    .select(
        "move_type",
        "distinct_operator_in_sku_id",
        "first_unit_price_to_unit_price_max_2_prior_ratio",
        "distinct_move_types",
        "total_unit_price_percent_rank",
        F.coalesce(F.col("distinct_move_types"), F.lit(0)).alias("distinct_move_types_filled"),
    )
)
