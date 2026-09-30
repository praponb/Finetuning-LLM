from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_move_types_moved_at = Window.partitionBy("distinct_move_types", F.month("moved_at_month_2")).orderBy("n_move_type_transfer")
w_distinct_move_types_moved_at_2 = Window.partitionBy("distinct_move_types", F.month("moved_at_month_2")).orderBy("n_move_type_transfer", "moved_at_month_2")
w_all_by_distinct_sku_ids_movi = Window.orderBy("distinct_sku_ids", "moved_at_month_2").rowsBetween(-6, Window.currentRow)
w_all_by_distinct_sku_ids_prio = Window.orderBy("distinct_sku_ids", "moved_at_month_2").rowsBetween(-3, -1)
w_n_move_type_transfer = Window.partitionBy("n_move_type_transfer")

prepared_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
        F.abs(F.col("qty") - 148).alias("qty_distance"),
    )
)

summary = (
    prepared_moves
    .filter(F.month(F.col("moved_at_month")).isin(1, 3))
    .groupBy(F.date_trunc("month", "moved_at").alias("moved_at_month_2"))
    .agg(
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.sum(F.when(F.col("move_type") != "TRANSFER", 1).otherwise(0)).alias("n_move_type_transfer"),
        F.count("*").alias("row_count"),
        F.sum("qty_distance").alias("total_qty_distance"),
        F.countDistinct("move_type").alias("distinct_move_types"),
    )
)

qualified = (
    summary
    .crossJoin(summary.agg(F.min("distinct_move_types").alias("overall_min_distinct_move_types")))
    .filter(F.col("distinct_move_types") > F.col("overall_min_distinct_move_types"))
    .drop("overall_min_distinct_move_types")
)

screened = (
    qualified
    .crossJoin(qualified.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

metrics = (
    screened
    .select(
        "*",
        F.dense_rank().over(w_distinct_move_types_moved_at).alias("n_move_type_transfer_dense_rank"),
        F.dense_rank().over(w_distinct_move_types_moved_at_2).alias("n_move_type_transfer_dense_rank_3"),
        F.round(F.avg("n_move_type_transfer").over(w_all_by_distinct_sku_ids_movi), 2).alias("n_move_type_transfer_avg_7"),
        F.sum("total_qty_distance").over(w_all_by_distinct_sku_ids_prio).alias("total_qty_distance_sum_3_prior"),
        F.min("total_qty_distance").over(w_n_move_type_transfer).alias("n_move_type_transfer_min_total_qty_distance"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.date_trunc("month", F.col("moved_at_month_2")).alias("moved_at_month_2_month"),
        F.date_trunc("year", F.col("moved_at_month_2")).alias("moved_at_month_2_year"),
    )
)

result = (
    enriched
    .select("total_qty_distance_sum_3_prior")
    .distinct()
    .orderBy("total_qty_distance_sum_3_prior")
)
