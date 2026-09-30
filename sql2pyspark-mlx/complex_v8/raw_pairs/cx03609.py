from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id_by_qty_ties = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc())
w_operator = Window.partitionBy("operator")
w_all_by_moved_at_moving_1 = Window.orderBy(F.col("moved_at").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc_nulls_first(), "id")
w_warehouse_id_by_qty_running = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_qty_moving_2 = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_unit_price_moving_6 = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(-6, Window.currentRow)
w_qty_rank_moved_at_quartile_b = Window.partitionBy("qty_rank", "moved_at_quartile").orderBy("qty_vs_operator_sku_id_avg")
w_all_by_unit_price_avg_2 = Window.orderBy("unit_price_avg_2", "id")
w_sku_id = Window.partitionBy("sku_id")
w_qty_rank_moved_at_quartile_b_2 = Window.partitionBy("qty_rank", "moved_at_quartile").orderBy("qty_vs_operator_sku_id_avg", "id").rowsBetween(-3, Window.currentRow)
w_all_by_min_qty_rank_ties = Window.orderBy(F.col("min_qty_rank").desc())
w_all_by_median_moved_at_quart = Window.orderBy(F.col("median_moved_at_quartile").desc(), "next_unit_price_dense_rank", "sku_id")
w_n_rows_min_qty_rank = Window.partitionBy("n_rows", "min_qty_rank")

base_moves = (
    df_stock_moves
    .filter(~F.col("move_type").isin("ADJUST", "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

ranked = (
    base_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        F.round(F.col("qty") - F.avg("qty").over(w_operator_sku_id), 2).alias("qty_vs_operator_sku_id_avg"),
        F.size(F.collect_set("warehouse_id").over(w_operator_sku_id)).alias("distinct_warehouse_id_in_operator_sku_id"),
        F.dense_rank().over(w_all_by_unit_price).alias("unit_price_dense_rank"),
        F.rank().over(w_warehouse_id_by_qty_ties).alias("qty_rank"),
        F.size(F.collect_set("warehouse_id").over(w_operator)).alias("distinct_warehouse_id_in_operator"),
        F.round(F.avg("unit_price").over(w_all_by_moved_at_moving_1), 2).alias("unit_price_avg_2"),
        F.ntile(4).over(w_all_by_moved_at).alias("moved_at_quartile"),
        F.sum("qty").over(w_operator).alias("operator_sum_qty"),
        F.min("qty").over(w_warehouse_id_by_qty_running).alias("min_qty_so_far"),
        F.max("qty").over(w_operator).alias("operator_max_qty"),
        F.sum("qty").over(w_warehouse_id_by_qty_moving_2).alias("qty_sum_3"),
        F.round(F.avg("unit_price").over(w_all_by_unit_price_moving_6), 2).alias("unit_price_avg_7"),
    )
)

labeled = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "qty_vs_operator_sku_id_avg",
        "unit_price_dense_rank",
        "qty_rank",
        "unit_price_avg_2",
        "moved_at_quartile",
        "min_qty_so_far",
        "qty_sum_3",
        "unit_price_avg_7",
        F.abs(F.col("operator_max_qty") - 3250).alias("operator_max_qty_distance"),
        (F.floor(F.col("min_qty_so_far") / 1000) * 1000).alias("min_qty_so_far_bucket"),
    )
)

ranked_2 = (
    labeled
    .select(
        "*",
        F.round(F.percent_rank().over(w_qty_rank_moved_at_quartile_b), 3).alias("qty_vs_operator_sku_id_avg_percent_rank"),
        (F.col("unit_price_avg_2") - F.lag("unit_price_avg_2").over(w_all_by_unit_price_avg_2)).alias("unit_price_avg_2_change"),
        F.lead("unit_price_dense_rank").over(w_all_by_unit_price_avg_2).alias("next_unit_price_dense_rank"),
        F.round(F.col("qty_vs_operator_sku_id_avg") / F.nullif(F.sum("qty_vs_operator_sku_id_avg").over(w_sku_id), F.lit(0)), 4).alias("qty_vs_operator_sku_id_avg_share_of_sku_id"),
        F.round(F.avg("moved_at_quartile").over(w_qty_rank_moved_at_quartile_b_2), 2).alias("moved_at_quartile_avg_4"),
        F.round(F.avg("unit_price").over(w_sku_id), 2).alias("sku_id_avg_unit_price"),
    )
)

per_group = (
    ranked_2
    .groupBy("next_unit_price_dense_rank", "sku_id")
    .agg(
        F.percentile_approx("moved_at_quartile", 0.5).alias("median_moved_at_quartile"),
        F.count("*").alias("n_rows"),
        F.min("qty_rank").alias("min_qty_rank"),
        F.percentile_approx("moved_at_quartile_avg_4", 0.5).alias("median_moved_at_quartile_avg_4"),
        F.round(F.stddev("moved_at_quartile"), 2).alias("stddev_moved_at_quartile"),
    )
    .filter(F.col("n_rows") >= 3)
)

sequenced = (
    per_group
    .select(
        "next_unit_price_dense_rank",
        "sku_id",
        "median_moved_at_quartile",
        "n_rows",
        "min_qty_rank",
        "median_moved_at_quartile_avg_4",
        "stddev_moved_at_quartile",
        F.round(F.percent_rank().over(w_all_by_min_qty_rank_ties), 3).alias("min_qty_rank_percent_rank"),
        F.rank().over(w_all_by_median_moved_at_quart).alias("median_moved_at_quartile_rank"),
        F.size(F.collect_set("sku_id").over(w_n_rows_min_qty_rank)).alias("distinct_sku_id_in_n_rows_min_qty_rank"),
        F.ntile(5).over(w_all_by_median_moved_at_quart).alias("median_moved_at_quartile_quintile"),
    )
)

result = (
    sequenced
    .filter(F.col("median_moved_at_quartile_avg_4").between(2.8, 6.4))
    .select(
        "next_unit_price_dense_rank",
        "sku_id",
        "median_moved_at_quartile",
        "n_rows",
        "median_moved_at_quartile_avg_4",
        "stddev_moved_at_quartile",
        "min_qty_rank_percent_rank",
        "median_moved_at_quartile_rank",
        "median_moved_at_quartile_quintile",
    )
    .orderBy(F.col("next_unit_price_dense_rank").desc_nulls_first(), "sku_id")
    .limit(5)
)
