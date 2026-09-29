from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_day_to_date_sku_id = Window.partitionBy(F.to_date("moved_at_day"), "sku_id")
w_all_by_moved_at_ties = Window.orderBy(F.col("moved_at").desc())
w_sku_id_by_unit_price_running = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_rank_move_type = Window.partitionBy("moved_at_rank", "move_type")
w_n_move_type_is_not_null_move = Window.partitionBy("n_move_type_is_not_null", "moved_at_year")
w_n_move_type_is_not_null_by_t = Window.partitionBy("n_move_type_is_not_null").orderBy("total_qty", "last_known_moved_at_day_month", "moved_at_year", "operator_list_item")
w_n_move_type_is_not_null_by_t_2 = Window.partitionBy("n_move_type_is_not_null").orderBy("total_qty", "last_known_moved_at_day_month", "moved_at_year", "operator_list_item").rowsBetween(-2, -1)

prepared_moves = (
    df_stock_moves
    .filter((F.col("unit_price") < 367.7) | (F.col("qty") <= 256))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
    )
)

metrics = (
    prepared_moves
    .select(
        "*",
        F.nth_value("unit_price", 2).over(w_all_by_moved_at_full).alias("second_unit_price"),
        F.count(F.lit(1)).over(w_moved_at_day_to_date_sku_id).alias("rows_in_moved_at_day_to_date_sku_id"),
        F.rank().over(w_all_by_moved_at_ties).alias("moved_at_rank"),
        F.round(F.percent_rank().over(w_all_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "moved_at_day",
        "second_unit_price",
        "moved_at_rank",
        "moved_at_percent_rank",
        F.last("moved_at_day", ignorenulls=True).over(w_sku_id_by_unit_price_running).alias("last_known_moved_at_day"),
        F.size(F.collect_set("operator").over(w_moved_at_rank_move_type)).alias("distinct_operator_in_moved_at_rank_move_type"),
        F.size(F.collect_set("warehouse_id").over(w_moved_at_rank_move_type)).alias("distinct_warehouse_id_in_moved_at_rank_move_type"),
        F.count(F.lit(1)).over(w_moved_at_rank_move_type).alias("rows_in_moved_at_rank_move_type"),
    )
)

grouped = (
    with_windows
    .groupBy(
        F.date_trunc("month", "last_known_moved_at_day").alias("last_known_moved_at_day_month"),
        F.year("moved_at").alias("moved_at_year"),
    )
    .agg(
        F.max("qty").alias("max_qty"),
        F.sum("qty").alias("total_qty"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.sum(F.when(F.col("move_type").isNotNull(), 1).otherwise(0)).alias("n_move_type_is_not_null"),
        F.round(F.sum("moved_at_percent_rank") / F.nullif(F.sum("second_unit_price"), F.lit(0)), 3).alias("moved_at_percent_rank_to_second_unit_price_ratio"),
    )
)

exploded = (
    grouped
    .select(
        "last_known_moved_at_day_month",
        "moved_at_year",
        "max_qty",
        "total_qty",
        "n_move_type_is_not_null",
        "moved_at_percent_rank_to_second_unit_price_ratio",
        F.explode("operator_list").alias("operator_list_item"),
    )
)

with_windows_2 = (
    exploded
    .select(
        "last_known_moved_at_day_month",
        "moved_at_year",
        "max_qty",
        "n_move_type_is_not_null",
        "moved_at_percent_rank_to_second_unit_price_ratio",
        "operator_list_item",
        F.round(F.col("total_qty") / F.nullif(F.sum("total_qty").over(w_n_move_type_is_not_null_move), F.lit(0)), 4).alias("total_qty_share_of_n_move_type_is_not_null_moved"),
        F.round(F.avg("moved_at_year").over(w_n_move_type_is_not_null_move), 2).alias("n_move_type_is_not_null_moved_at_year_avg_moved_"),
        F.ntile(5).over(w_n_move_type_is_not_null_by_t).alias("total_qty_quintile"),
        F.round(F.avg("moved_at_year").over(w_n_move_type_is_not_null_by_t_2), 2).alias("moved_at_year_avg_2_prior"),
    )
)

enriched = (
    with_windows_2
    .select(
        "last_known_moved_at_day_month",
        "moved_at_year",
        "max_qty",
        "operator_list_item",
        "total_qty_share_of_n_move_type_is_not_null_moved",
        "n_move_type_is_not_null_moved_at_year_avg_moved_",
        "total_qty_quintile",
        F.when(F.col("n_move_type_is_not_null") >= 7, "HIGH").when(F.col("n_move_type_is_not_null") >= 2, "MEDIUM").otherwise("LOW").alias("n_move_type_is_not_null_tier"),
    )
)

result = (
    enriched
    .select(
        "last_known_moved_at_day_month",
        "moved_at_year",
        "operator_list_item",
        "total_qty_share_of_n_move_type_is_not_null_moved",
        "n_move_type_is_not_null_moved_at_year_avg_moved_",
        "total_qty_quintile",
        "n_move_type_is_not_null_tier",
        F.round(F.col("total_qty_quintile") / F.nullif(F.col("moved_at_year"), F.lit(0)), 3).alias("total_qty_quintile_per_moved_at_year"),
    )
)
