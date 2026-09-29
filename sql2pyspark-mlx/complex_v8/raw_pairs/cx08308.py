from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_prior_3 = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(-3, -1)
w_move_type_qty_tier = Window.partitionBy("move_type", "qty_tier")
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_all_by_unit_price_running = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_running = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_ties = Window.orderBy(F.col("moved_at").desc())
w_all_by_moved_at_prior_2 = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(-2, -1)
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_all_by_moved_at_moving_3 = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_unit_price_prior_2 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(-2, -1)
w_move_type_by_second_unit_pri = Window.partitionBy("move_type").orderBy("second_unit_price", "id")
w_moved_at_seq_warehouse_id = Window.partitionBy("moved_at_seq", "warehouse_id")
w_move_type_sku_id_by_unit_pri = Window.partitionBy("move_type", "sku_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_unit_price_max_2_p = Window.partitionBy("sku_id").orderBy(F.col("unit_price_max_2_prior").desc_nulls_first(), "id")
w_sku_id_by_unit_price_max_2_p_2 = Window.partitionBy("sku_id").orderBy(F.col("unit_price_max_2_prior").desc_nulls_first())
w_all_by_pct_move_type_transfe = Window.orderBy(F.col("pct_move_type_transfer").asc_nulls_last(), "unit_price_tier", "warehouse_id", "moved_at_day")
w_all_by_first_moved_at = Window.orderBy(F.col("first_moved_at").desc(), "unit_price_tier", "warehouse_id", "moved_at_day")

base = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(2, 3)) & (F.col("warehouse_id") <= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.when(F.col("qty") >= 393, "A").when(F.col("qty") >= 191, "B").otherwise("C").alias("qty_tier"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.max("unit_price").over(w_all_by_moved_at_prior_3).alias("unit_price_max_3_prior"),
        F.round(F.avg("unit_price").over(w_move_type_qty_tier), 2).alias("move_type_qty_tier_avg_unit_price"),
        F.row_number().over(w_all_by_moved_at).alias("moved_at_seq"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type_qty_tier), 2).alias("unit_price_vs_move_type_qty_tier_avg"),
        F.first("unit_price", ignorenulls=True).over(w_all_by_unit_price_running).alias("first_known_unit_price"),
        F.count(F.lit(1)).over(w_move_type_qty_tier).alias("rows_in_move_type_qty_tier"),
        F.count(F.lit(1)).over(w_all_by_moved_at_running).alias("running_count"),
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.sum("qty").over(w_all_by_moved_at_prior_2).alias("qty_sum_2_prior"),
        F.last("unit_price").over(w_all_by_moved_at_full).alias("last_unit_price"),
        F.round(F.percent_rank().over(w_all_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
        F.nth_value("unit_price", 2).over(w_all_by_moved_at_full).alias("second_unit_price"),
        F.round(F.avg("unit_price").over(w_all_by_moved_at_moving_3), 2).alias("unit_price_avg_4"),
        F.max("unit_price").over(w_all_by_unit_price_prior_2).alias("unit_price_max_2_prior"),
    )
)

first_rows = with_windows.filter(F.col("moved_at_seq") <= 2)

windowed = (
    first_rows
    .select(
        "*",
        F.lead("moved_at").over(w_move_type_by_second_unit_pri).alias("next_moved_at"),
        F.round(F.col("last_unit_price") - F.avg("last_unit_price").over(w_moved_at_seq_warehouse_id), 2).alias("last_unit_price_vs_moved_at_seq_warehouse_id_avg"),
        F.nth_value("unit_price_avg_4", 2).over(w_move_type_sku_id_by_unit_pri).alias("second_unit_price_avg_4"),
        F.lead("qty", 3).over(w_move_type_by_second_unit_pri).alias("next3_qty"),
        F.dense_rank().over(w_sku_id_by_unit_price_max_2_p).alias("unit_price_max_2_prior_dense_rank"),
        F.dense_rank().over(w_sku_id_by_unit_price_max_2_p_2).alias("unit_price_max_2_prior_dense_rank_2"),
    )
)

rollup_base = (
    windowed
    .groupBy(
        F.when(F.col("unit_price") >= 330.0, "A").when(F.col("unit_price") >= 209.4, "B").otherwise("C").alias("unit_price_tier"),
        "warehouse_id",
        F.to_date("moved_at").alias("moved_at_day"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("move_type") != "TRANSFER", 1)) / F.count("*"), 1).alias("pct_move_type_transfer"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("moved_at").alias("first_moved_at"),
        F.max("qty_sum_2_prior").alias("max_qty_sum_2_prior_check"),
    )
    .filter(F.col("max_qty_sum_2_prior_check") > 301)
    .drop("max_qty_sum_2_prior_check")
)

ranked = (
    rollup_base
    .withColumn("pct_move_type_transfer_cume_dist", F.round(F.cume_dist().over(w_all_by_pct_move_type_transfe), 3))
    .withColumn("first_moved_at_percent_rank", F.round(F.percent_rank().over(w_all_by_first_moved_at), 3))
)

result = (
    ranked
    .select(
        "unit_price_tier",
        "distinct_ids",
        "pct_move_type_transfer_cume_dist",
        F.abs(F.col("distinct_ids") - 4).alias("distinct_ids_distance"),
    )
    .orderBy(F.col("pct_move_type_transfer_cume_dist").asc_nulls_last())
)
