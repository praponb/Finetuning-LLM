from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_by_qty_distance = Window.partitionBy("warehouse_id").orderBy(F.col("qty_distance").desc(), "id")
w_all_by_qty_distance_ties = Window.orderBy("qty_distance")
w_sku_is_active_by_moved_at = Window.partitionBy("sku_is_active").orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_all_by_qty_distance = Window.orderBy("qty_distance", "id")
w_moved_at_year_warehouse_id = Window.partitionBy(F.year("moved_at"), "warehouse_id")
w_warehouse_id_by_qty_distance_2 = Window.partitionBy("warehouse_id").orderBy(F.col("qty_distance").desc())
w_all_by_qty_distance_moving_1 = Window.orderBy("qty_distance", "id").rowsBetween(-1, Window.currentRow)
w_warehouse_id_operator_by_qty = Window.partitionBy("warehouse_id", "operator").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_qty_distance_dense_rank_2_by = Window.partitionBy("qty_distance_dense_rank_2").orderBy(F.col("qty_distance_dense_rank").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn = Window.partitionBy("rn")
w_qty_distance_dense_rank_2_by_2 = Window.partitionBy("qty_distance_dense_rank_2").orderBy(F.col("qty_distance_dense_rank").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_next_qty = Window.orderBy("next_qty", "id")

filtered_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.when(F.col("unit_price") >= 363.6, "large").when(F.col("unit_price") >= 130.6, "medium").otherwise("small").alias("unit_price_tier"),
        F.abs(F.col("qty") - 244).alias("qty_distance"),
    )
)

joined = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", F.col("sk.is_active").alias("sku_is_active"), "sk.sku_code")
)

windowed = (
    joined
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "qty_distance",
        "sku_code",
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.row_number().over(w_warehouse_id_by_qty_distance).alias("rn"),
        F.dense_rank().over(w_all_by_qty_distance_ties).alias("qty_distance_dense_rank"),
        F.ntile(5).over(w_sku_is_active_by_moved_at).alias("moved_at_quintile"),
        F.lead("qty", 1, 0).over(w_all_by_qty_distance).alias("next_qty"),
        F.size(F.collect_set("unit_price_tier").over(w_moved_at_year_warehouse_id)).alias("distinct_unit_price_tier_in_moved_at_year_wareho"),
        F.dense_rank().over(w_warehouse_id_by_qty_distance_2).alias("qty_distance_dense_rank_2"),
        F.round(F.avg("qty_distance").over(w_all_by_qty_distance_moving_1), 2).alias("qty_distance_avg_2"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_moved_at_year_warehouse_id), F.lit(0)), 4).alias("qty_share_of_moved_at_year_warehouse_id"),
        F.sum("qty").over(w_sku_id).alias("sku_id_sum_qty"),
        F.nth_value("qty_distance", 2).over(w_warehouse_id_operator_by_qty).alias("second_qty_distance"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.last("next_qty").over(w_qty_distance_dense_rank_2_by).alias("last_next_qty"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_rn), F.lit(0)), 4).alias("qty_share_of_rn"),
        F.min("qty_distance").over(w_qty_distance_dense_rank_2_by_2).alias("min_qty_distance_so_far"),
        F.round(F.col("sku_id_avg_qty") / F.nullif(F.sum("sku_id_avg_qty").over(w_rn), F.lit(0)), 4).alias("sku_id_avg_qty_share_of_rn"),
        F.rank().over(w_all_by_next_qty).alias("next_qty_rank"),
        F.nth_value("moved_at_quintile", 2).over(w_qty_distance_dense_rank_2_by).alias("second_moved_at_quintile"),
        F.round(F.avg("qty_distance").over(w_rn), 2).alias("rn_avg_qty_distance"),
        F.max("moved_at_quintile").over(w_rn).alias("rn_max_moved_at_quintile"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "qty_distance",
        "sku_code",
        "sku_id_avg_qty",
        "rn",
        "moved_at_quintile",
        "next_qty",
        "distinct_unit_price_tier_in_moved_at_year_wareho",
        "qty_distance_dense_rank_2",
        "qty_distance_avg_2",
        "qty_share_of_moved_at_year_warehouse_id",
        "second_qty_distance",
        "last_next_qty",
        "qty_share_of_rn",
        "min_qty_distance_so_far",
        "sku_id_avg_qty_share_of_rn",
        "next_qty_rank",
        "second_moved_at_quintile",
        "rn_avg_qty_distance",
        "rn_max_moved_at_quintile",
    )
    .orderBy(F.col("rn").asc())
)
