from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator_len = Window.partitionBy("warehouse_id", "operator_len")
w_move_type_by_days_since_move = Window.partitionBy("move_type").orderBy("days_since_moved_at")
w_move_type_by_days_since_move_2 = Window.partitionBy("move_type").orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_days_since_move_3 = Window.partitionBy("move_type").orderBy("days_since_moved_at", "id")
w_all_by_n_operator_is_not_nul = Window.orderBy(F.col("n_operator_is_not_null").desc(), "moved_at_month", "days_since_moved_at_dense_rank")
w_n_operator_is_not_null = Window.partitionBy("n_operator_is_not_null")
w_all_by_n_operator_is_not_nul_2 = Window.orderBy(F.col("n_operator_is_not_null").desc(), "moved_at_month", "days_since_moved_at_dense_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_operator_is_not_nul_3 = Window.orderBy(F.col("n_operator_is_not_null").desc(), "moved_at_month", "days_since_moved_at_dense_rank").rowsBetween(-2, -1)
w_all_by_n_operator_is_not_nul_4 = Window.orderBy(F.col("n_operator_is_not_null").desc(), "moved_at_month", "days_since_moved_at_dense_rank").rowsBetween(-3, Window.currentRow)

valid = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")) | (F.col("qty") >= 93))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.min("operator_len").over(w_warehouse_id_operator_len).alias("warehouse_id_operator_len_min_operator_len"),
        F.dense_rank().over(w_move_type_by_days_since_move).alias("days_since_moved_at_dense_rank"),
        F.last("moved_at_day").over(w_move_type_by_days_since_move_2).alias("last_moved_at_day"),
        F.round(F.col("operator_len") - F.avg("operator_len").over(w_warehouse_id_operator_len), 2).alias("operator_len_vs_warehouse_id_operator_len_avg"),
        F.round(F.col("operator_len") / F.nullif(F.sum("operator_len").over(w_warehouse_id_operator_len), F.lit(0)), 4).alias("operator_len_share_of_warehouse_id_operator_len"),
        F.lead("moved_at_day").over(w_move_type_by_days_since_move_3).alias("next_moved_at_day"),
    )
)

rollup_base = (
    windowed
    .groupBy(
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
        "days_since_moved_at_dense_rank",
    )
    .agg(
        F.count(F.when(F.col("operator").isNotNull(), 1)).alias("n_operator_is_not_null"),
        F.percentile_approx("operator_len_vs_warehouse_id_operator_len_avg", 0.5).alias("median_operator_len_vs_warehouse_id_operator_len"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.max("operator_len_vs_warehouse_id_operator_len_avg").alias("max_operator_len_vs_warehouse_id_operator_len_av"),
    )
    .filter(F.col("max_operator_len_vs_warehouse_id_operator_len_av") > -9.8)
    .drop("max_operator_len_vs_warehouse_id_operator_len_av")
)

metrics = (
    rollup_base
    .select(
        "*",
        F.ntile(10).over(w_all_by_n_operator_is_not_nul).alias("n_operator_is_not_null_decile"),
        F.first("n_operator_is_not_null", ignorenulls=True).over(w_all_by_n_operator_is_not_nul_2).alias("first_known_n_operator_is_not_null"),
        F.round(F.avg("distinct_warehouse_ids").over(w_all_by_n_operator_is_not_nul_3), 2).alias("distinct_warehouse_ids_avg_2_prior"),
        F.round(F.avg("distinct_warehouse_ids").over(w_all_by_n_operator_is_not_nul_4), 2).alias("distinct_warehouse_ids_avg_4"),
        F.ntile(4).over(w_all_by_n_operator_is_not_nul).alias("n_operator_is_not_null_quartile"),
    )
)

derived = (
    metrics
    .select(
        "*",
        F.coalesce(F.col("distinct_warehouse_ids"), F.lit(0)).alias("distinct_warehouse_ids_filled"),
        F.abs(F.col("distinct_warehouse_ids") - 5).alias("distinct_warehouse_ids_distance"),
    )
)

result = (
    derived
    .select(
        "moved_at_month",
        "days_since_moved_at_dense_rank",
        "median_operator_len_vs_warehouse_id_operator_len",
        "distinct_warehouse_ids",
        "n_operator_is_not_null_decile",
        "first_known_n_operator_is_not_null",
        "distinct_warehouse_ids_avg_2_prior",
        "distinct_warehouse_ids_avg_4",
        "distinct_warehouse_ids_filled",
        "distinct_warehouse_ids_distance",
        (F.floor(F.col("n_operator_is_not_null") / 2) * 2).alias("n_operator_is_not_null_bucket"),
    )
    .orderBy(
        F.col("median_operator_len_vs_warehouse_id_operator_len").desc(),
        F.col("distinct_warehouse_ids").desc(),
    )
)
