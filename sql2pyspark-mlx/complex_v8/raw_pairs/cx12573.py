from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_bucket_tie = Window.orderBy(F.col("unit_price_bucket").desc())
w_sku_id = Window.partitionBy("sku_id")
w_operator_by_moved_at_ties = Window.partitionBy("operator").orderBy("moved_at")
w_operator_by_moved_at_moving = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_unit_price_bucket_ful = Window.orderBy(F.col("unit_price_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_warehouse_id_by_un = Window.partitionBy("move_type", "warehouse_id").orderBy("unit_price_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price_bucket_pri = Window.orderBy(F.col("unit_price_bucket").desc(), "id").rowsBetween(-1, -1)
w_move_type_warehouse_id_by_un_2 = Window.partitionBy("move_type", "warehouse_id").orderBy("unit_price_bucket", "id")
w_operator_by_moved_at_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_moves = (
    df_stock_moves
    .filter((F.col("qty").between(85, 468)) & (F.col("operator").isin("op_dew", "op_ann")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

sequenced = (
    base_moves
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_unit_price_bucket_tie), 3).alias("unit_price_bucket_percent_rank"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id), F.lit(0)), 4).alias("unit_price_share_of_sku_id"),
        F.dense_rank().over(w_operator_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.round(F.avg("qty").over(w_operator_by_moved_at_moving), 2).alias("qty_avg_3"),
        F.nth_value("qty", 2).over(w_all_by_unit_price_bucket_ful).alias("second_qty"),
        F.last("qty").over(w_move_type_warehouse_id_by_un).alias("last_qty"),
        F.min("unit_price").over(w_sku_id).alias("sku_id_min_unit_price"),
        F.round(F.col("unit_price_bucket") / F.nullif(F.sum("unit_price_bucket").over(w_sku_id), F.lit(0)), 4).alias("unit_price_bucket_share_of_sku_id"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
        F.round(F.avg("qty").over(w_all_by_unit_price_bucket_pri), 2).alias("qty_avg_1_prior"),
        F.round(F.cume_dist().over(w_move_type_warehouse_id_by_un_2), 3).alias("unit_price_bucket_cume_dist"),
        F.last("moved_at", ignorenulls=True).over(w_operator_by_moved_at_running).alias("last_known_moved_at"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.size(F.collect_set("operator").over(w_sku_id)).alias("distinct_operator_in_sku_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_bucket",
        "unit_price_bucket_percent_rank",
        "unit_price_share_of_sku_id",
        "qty_avg_3",
        "second_qty",
        "sku_id_min_unit_price",
        "unit_price_bucket_share_of_sku_id",
        "distinct_warehouse_id_in_sku_id",
        "qty_avg_1_prior",
        "last_known_moved_at",
        "distinct_operator_in_sku_id",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
    )
    .orderBy(
        F.col("distinct_warehouse_id_in_sku_id").desc(),
        F.col("operator").asc(),
    )
)
