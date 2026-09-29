from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_unit_price_tier_by_moved_at = Window.partitionBy("unit_price_tier").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_unit_price_tier_sku_id = Window.partitionBy("unit_price_tier", "sku_id")
w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy(F.col("moved_at").desc())
w_unit_price_tier_by_moved_at_2 = Window.partitionBy("unit_price_tier").orderBy("moved_at", "id")
w_all_by_moved_at_ties = Window.orderBy(F.col("moved_at").desc())
w_unit_price_tier_by_moved_at_3 = Window.partitionBy("unit_price_tier").orderBy("moved_at")
w_sku_id_move_type_by_moved_at_2 = Window.partitionBy("sku_id", "move_type").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_sku_id = Window.partitionBy("sku_id")
w_sku_id_move_type_by_moved_at_3 = Window.partitionBy("sku_id", "move_type").orderBy(F.col("moved_at").desc(), "id")
w_all_by_qty_running = Window.orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") >= 5) & (F.col("unit_price").between(125.9, 387.1)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.concat(F.col("move_type"), F.lit("#"), F.col("qty").cast("string")).alias("move_type_ref"),
        F.when(F.col("unit_price") >= 330.9, "A").when(F.col("unit_price") >= 189.4, "B").otherwise("C").alias("unit_price_tier"),
    )
)

with_windows = (
    base_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "move_type_ref",
        F.count(F.lit(1)).over(w_unit_price_tier_by_moved_at).alias("running_count"),
        F.sum("qty").over(w_unit_price_tier_sku_id).alias("unit_price_tier_sku_id_sum_qty"),
        F.dense_rank().over(w_sku_id_move_type_by_moved_at).alias("moved_at_dense_rank"),
        F.lead("qty").over(w_unit_price_tier_by_moved_at_2).alias("next_qty"),
        F.round(F.percent_rank().over(w_all_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
        F.round(F.cume_dist().over(w_unit_price_tier_by_moved_at_3), 3).alias("moved_at_cume_dist"),
        F.round(F.avg("qty").over(w_unit_price_tier_sku_id), 2).alias("unit_price_tier_sku_id_avg_qty"),
        F.round(F.avg("qty").over(w_sku_id_move_type_by_moved_at_2), 2).alias("qty_avg_2"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id), 2).alias("qty_vs_sku_id_avg"),
        F.lead("moved_at").over(w_unit_price_tier_by_moved_at_2).alias("next_moved_at"),
        F.round(F.col("qty") - F.avg("qty").over(w_unit_price_tier_sku_id), 2).alias("qty_vs_unit_price_tier_sku_id_avg"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_sku_id_move_type_by_moved_at_3))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.count(F.lit(1)).over(w_all_by_qty_running).alias("running_count_2"),
    )
)

totals = (
    with_windows
    .filter(F.col("qty_avg_2") > 125.8)
    .groupBy("sku_id")
    .agg(
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.min("next_moved_at").alias("first_next_moved_at"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("qty_vs_sku_id_avg") / F.nullif(F.sum("moved_at_percent_rank"), F.lit(0)), 3).alias("qty_vs_sku_id_avg_to_moved_at_percent_rank_ratio"),
        F.sum("qty_avg_2").alias("sum_qty_avg_2_check"),
    )
    .filter(F.col("sum_qty_avg_2_check") > 125.8)
    .drop("sum_qty_avg_2_check")
)

result = (
    totals
    .select(
        "sku_id",
        "distinct_sku_ids",
        "first_next_moved_at",
        "qty_vs_sku_id_avg_to_moved_at_percent_rank_ratio",
        F.coalesce(F.col("distinct_sku_ids"), F.lit(0)).alias("distinct_sku_ids_filled"),
    )
)
