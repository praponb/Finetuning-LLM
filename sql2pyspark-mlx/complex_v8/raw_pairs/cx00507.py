from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_ties = Window.orderBy(F.col("moved_at").desc())
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_move_type_or_unknown_move_ty = Window.partitionBy("move_type_or_unknown", "move_type").orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_move_type_or_unknown_move_ty_2 = Window.partitionBy("move_type_or_unknown", "move_type").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_warehouse_id_by_unit_price_p = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-1, -1)
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_moved_at_month_move_type_by = Window.partitionBy(F.month("moved_at"), "move_type").orderBy(F.col("unit_price_vs_warehouse_id_move_type_avg").desc(), "id")
w_all_by_unit_price_prior_2 = Window.orderBy("unit_price", "id").rowsBetween(-2, -1)
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_unit_price_avg_1_prio = Window.orderBy("unit_price_avg_1_prior", "id").rowsBetween(-3, Window.currentRow)
w_sku_id = Window.partitionBy("sku_id")

filtered_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-31 07:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

ranked = (
    filtered_moves
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
        F.max("unit_price").over(w_warehouse_id_move_type).alias("warehouse_id_move_type_max_unit_price"),
        F.round(F.avg("unit_price").over(w_warehouse_id_move_type), 2).alias("warehouse_id_move_type_avg_unit_price"),
        (F.col("unit_price") - F.lag("unit_price").over(w_move_type_or_unknown_move_ty)).alias("unit_price_change"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_move_type), 2).alias("unit_price_vs_warehouse_id_move_type_avg"),
        F.round(F.avg("unit_price").over(w_move_type_or_unknown_move_ty_2), 2).alias("unit_price_avg_2_prior"),
        F.count(F.lit(1)).over(w_warehouse_id_move_type).alias("rows_in_warehouse_id_move_type"),
        F.round(F.avg("unit_price").over(w_warehouse_id_by_unit_price_p), 2).alias("unit_price_avg_1_prior"),
        F.row_number().over(w_warehouse_id_by_unit_price).alias("rn"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at_percent_rank",
        "warehouse_id_move_type_avg_unit_price",
        "unit_price_change",
        "unit_price_vs_warehouse_id_move_type_avg",
        "unit_price_avg_2_prior",
        "rows_in_warehouse_id_move_type",
        "unit_price_avg_1_prior",
        "rn",
        F.ntile(4).over(w_moved_at_month_move_type_by).alias("unit_price_vs_warehouse_id_move_type_avg_quartil"),
        F.round(F.avg("unit_price_avg_1_prior").over(w_all_by_unit_price_prior_2), 2).alias("unit_price_avg_1_prior_avg_2_prior"),
        F.ntile(5).over(w_all_by_unit_price).alias("unit_price_quintile"),
        F.round(F.avg("unit_price_avg_1_prior").over(w_all_by_unit_price_avg_1_prio), 2).alias("unit_price_avg_1_prior_avg_4"),
        F.size(F.collect_set("move_type_or_unknown").over(w_sku_id)).alias("distinct_move_type_or_unknown_in_sku_id"),
    )
)

grouped = (
    windowed
    .groupBy("rn")
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("move_type").isin("ADJUST", "TRANSFER"), 1)).alias("n_move_type_in_adjust_transfer"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.round(F.avg("rn"), 2).alias("avg_rn"),
        F.percentile_approx("distinct_move_type_or_unknown_in_sku_id", 0.5).alias("median_distinct_move_type_or_unknown_in_sku_id"),
        F.avg("unit_price_avg_1_prior_avg_4").alias("avg_unit_price_avg_1_prior_avg_4_check"),
    )
    .filter(F.col("avg_unit_price_avg_1_prior_avg_4_check") > 150.7)
    .drop("avg_unit_price_avg_1_prior_avg_4_check")
)

result = (
    grouped
    .select(
        "rn",
        "row_count",
        "n_move_type_in_adjust_transfer",
        "median_distinct_move_type_or_unknown_in_sku_id",
        F.coalesce(F.col("rn"), F.lit(0)).alias("rn_filled"),
        (F.col("row_count") - F.col("avg_rn")).alias("row_count_minus_avg_rn"),
    )
    .orderBy("rn_filled")
)
