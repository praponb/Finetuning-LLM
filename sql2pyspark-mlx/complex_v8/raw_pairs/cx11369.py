from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_operator_by_qty_per_unit_pri = Window.partitionBy("operator").orderBy("qty_per_unit_price")
w_warehouse_id_unit_price_tier = Window.partitionBy("warehouse_id", "unit_price_tier").orderBy(F.col("qty_per_unit_price").desc(), "id")
w_warehouse_id_unit_price_tier_2 = Window.partitionBy("warehouse_id", "unit_price_tier").orderBy(F.col("qty_per_unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_move_type_by_moved_at_ties = Window.partitionBy("move_type").orderBy(F.col("moved_at").desc())
w_operator_by_qty_per_unit_pri_2 = Window.partitionBy("operator").orderBy("qty_per_unit_price", "id")
w_warehouse_id_by_qty_per_unit = Window.partitionBy("warehouse_id").orderBy(F.col("qty_per_unit_price_rank").desc(), "id")
w_distinct_move_type_in_sku_id = Window.partitionBy("distinct_move_type_in_sku_id", "warehouse_id").orderBy(F.col("sku_id_avg_qty").desc())
w_distinct_move_type_in_sku_id_2 = Window.partitionBy("distinct_move_type_in_sku_id", "warehouse_id").orderBy(F.col("sku_id_avg_qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_moves = (
    df_stock_moves
    .filter(F.col("move_type").isin("TRANSFER", "INBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 289.7, "HIGH").when(F.col("unit_price") >= 209.4, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

with_windows = (
    recent_moves
    .select(
        "*",
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_id), F.lit(0)), 4).alias("qty_share_of_sku_id"),
        F.max("qty").over(w_sku_id).alias("sku_id_max_qty"),
        F.rank().over(w_operator_by_qty_per_unit_pri).alias("qty_per_unit_price_rank"),
        F.size(F.collect_set("move_type").over(w_sku_id)).alias("distinct_move_type_in_sku_id"),
        F.row_number().over(w_warehouse_id_unit_price_tier).alias("rn"),
        F.sum("qty").over(w_warehouse_id_unit_price_tier_2).alias("running_qty"),
        F.round(F.col("qty_per_unit_price") - F.avg("qty_per_unit_price").over(w_warehouse_id), 2).alias("qty_per_unit_price_vs_warehouse_id_avg"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id), F.lit(0)), 4).alias("unit_price_share_of_sku_id"),
        F.round(F.percent_rank().over(w_move_type_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
        F.dense_rank().over(w_move_type_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.round(F.cume_dist().over(w_operator_by_qty_per_unit_pri_2), 3).alias("qty_per_unit_price_cume_dist"),
    )
)

sequenced = (
    with_windows
    .select(
        "*",
        (F.col("qty_per_unit_price") - F.lag("qty_per_unit_price").over(w_warehouse_id_by_qty_per_unit)).alias("qty_per_unit_price_change"),
        F.dense_rank().over(w_distinct_move_type_in_sku_id).alias("sku_id_avg_qty_dense_rank"),
        F.lag("moved_at").over(w_warehouse_id_by_qty_per_unit).alias("prev_moved_at"),
        F.last("unit_price").over(w_distinct_move_type_in_sku_id_2).alias("last_unit_price"),
    )
)

stats = (
    sequenced
    .groupBy(F.to_date("moved_at").alias("moved_at_day"))
    .agg(
        F.min("qty").alias("min_qty"),
        F.round(100 * F.count(F.when(F.col("move_type") == "ADJUST", 1)) / F.count("*"), 1).alias("pct_move_type_adjust"),
        F.max("moved_at").alias("last_moved_at"),
        F.round(F.sum("unit_price") / F.nullif(F.sum("sku_id_avg_qty_dense_rank"), F.lit(0)), 3).alias("unit_price_to_sku_id_avg_qty_dense_rank_ratio"),
        F.round(F.avg("unit_price_share_of_sku_id"), 2).alias("avg_unit_price_share_of_sku_id"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
)

result = (
    stats
    .select(
        "moved_at_day",
        "min_qty",
        "pct_move_type_adjust",
        "last_moved_at",
        "avg_unit_price_share_of_sku_id",
        "operator_list",
        (F.col("last_moved_at") + F.expr("INTERVAL 7 DAYS")).alias("last_moved_at_plus_7d"),
        F.coalesce(F.col("min_qty"), F.lit(0)).alias("min_qty_filled"),
    )
    .orderBy(
        F.col("avg_unit_price_share_of_sku_id").asc_nulls_last(),
        F.col("last_moved_at").desc_nulls_first(),
    )
)
