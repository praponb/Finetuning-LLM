from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_sku_id = Window.partitionBy("operator", "sku_id")
w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_move_type_by_moved = Window.partitionBy("operator", "move_type").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_operator_move_type_by_moved_2 = Window.partitionBy("operator", "move_type").orderBy(F.col("moved_at").desc(), "id")
w_warehouse_id_sku_id_by_moved_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy("moved_at")
w_operator_move_type_by_moved_3 = Window.partitionBy("operator", "move_type").orderBy(F.col("moved_at").desc())
w_operator_move_type_by_moved_4 = Window.partitionBy("operator", "move_type").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_bucket_running = Window.orderBy("qty_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_bucket = Window.orderBy("qty_bucket", "id")

scoped_moves = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-03-08").cast("date")) & (F.col("warehouse_id") >= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

windowed = (
    scoped_moves
    .select(
        "*",
        F.sum("qty_bucket").over(w_operator_sku_id).alias("operator_sku_id_sum_qty_bucket"),
        F.max("days_since_moved_at").over(w_operator_sku_id).alias("operator_sku_id_max_days_since_moved_at"),
        F.nth_value("qty_bucket", 2).over(w_warehouse_id_sku_id_by_moved).alias("second_qty_bucket"),
        F.max("qty_bucket").over(w_operator_move_type_by_moved).alias("qty_bucket_max_7"),
        F.round(F.avg("qty_bucket").over(w_operator_move_type_by_moved), 2).alias("qty_bucket_avg_7"),
        F.round(F.col("days_since_moved_at") / F.nullif(F.sum("days_since_moved_at").over(w_operator_sku_id), F.lit(0)), 4).alias("days_since_moved_at_share_of_operator_sku_id"),
        F.round(F.cume_dist().over(w_operator_move_type_by_moved_2), 3).alias("moved_at_cume_dist"),
        F.round(F.percent_rank().over(w_warehouse_id_sku_id_by_moved_2), 3).alias("moved_at_percent_rank"),
        F.dense_rank().over(w_operator_move_type_by_moved_3).alias("moved_at_dense_rank"),
        F.min("qty_bucket").over(w_operator_sku_id).alias("operator_sku_id_min_qty_bucket"),
        F.round(F.cume_dist().over(w_operator_move_type_by_moved_3), 3).alias("moved_at_cume_dist_2"),
        F.first("moved_at", ignorenulls=True).over(w_operator_move_type_by_moved_4).alias("first_known_moved_at"),
        F.last("moved_at", ignorenulls=True).over(w_all_by_qty_bucket_running).alias("last_known_moved_at"),
        F.lag("moved_at").over(w_all_by_qty_bucket).alias("prev_moved_at"),
    )
)

summary = (
    windowed
    .groupBy(
        F.when(F.col("second_qty_bucket") >= 300, "large").when(F.col("second_qty_bucket") >= 151, "medium").otherwise("small").alias("second_qty_bucket_tier"),
    )
    .agg(
        F.max("prev_moved_at").alias("last_prev_moved_at"),
        F.round(F.sum("moved_at_dense_rank") / F.nullif(F.sum("operator_sku_id_sum_qty_bucket"), F.lit(0)), 3).alias("moved_at_dense_rank_to_operator_sku_id_sum_qty_b"),
        F.count(F.when(F.col("operator").isin("op_eve", "op_bee"), 1)).alias("n_operator_in_op_eve_op_bee"),
        F.max("qty_bucket_avg_7").alias("max_qty_bucket_avg_7_check"),
    )
    .filter(F.col("max_qty_bucket_avg_7_check") > 125.8)
    .drop("max_qty_bucket_avg_7_check")
)

result = (
    summary
    .select(
        "second_qty_bucket_tier",
        F.when(F.col("second_qty_bucket_tier").isin("medium", "large"), "group_a").when(F.col("second_qty_bucket_tier") == "small", "group_b").otherwise("other").alias("second_qty_bucket_tier_group"),
        F.when(F.col("second_qty_bucket_tier").isin("large", "medium"), "group_a").when(F.col("second_qty_bucket_tier") == "small", "group_b").otherwise("other").alias("second_qty_bucket_tier_group_2"),
    )
)
