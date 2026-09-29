from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_week_month_by_unit = Window.partitionBy(F.date_trunc("month", "moved_at_week")).orderBy(F.col("unit_price_bucket").desc())
w_moved_at_week_month_by_unit_2 = Window.partitionBy(F.date_trunc("month", "moved_at_week")).orderBy(F.col("unit_price_bucket").desc(), "id").rowsBetween(-6, -1)
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_moved_at_week_month_by_unit_3 = Window.partitionBy(F.date_trunc("month", "moved_at_week")).orderBy(F.col("unit_price_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_move_type = Window.partitionBy("operator", "move_type")
w_move_type_by_moved_at_week_r = Window.partitionBy("move_type").orderBy("moved_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_unit_price_ties = Window.partitionBy("operator").orderBy("unit_price")
w_operator_by_unit_price_movin = Window.partitionBy("operator").orderBy("unit_price", "id").rowsBetween(-1, Window.currentRow)
w_move_type = Window.partitionBy("move_type")
w_sku_id_by_unit_price_bucket = Window.partitionBy("sku_id").orderBy("unit_price_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_moves = (
    df_stock_moves
    .filter((F.col("qty") < 349) | (F.col("warehouse_id") >= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

with_windows = (
    valid_moves
    .select(
        "*",
        F.round(F.cume_dist().over(w_moved_at_week_month_by_unit), 3).alias("unit_price_bucket_cume_dist"),
        F.round(F.avg("unit_price").over(w_moved_at_week_month_by_unit_2), 2).alias("unit_price_avg_6_prior"),
        F.ntile(10).over(w_warehouse_id_by_unit_price).alias("unit_price_decile"),
        F.last("unit_price_bucket", ignorenulls=True).over(w_moved_at_week_month_by_unit_3).alias("last_known_unit_price_bucket"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator_move_type), F.lit(0)), 4).alias("unit_price_share_of_operator_move_type"),
        F.max("unit_price_bucket").over(w_move_type_by_moved_at_week_r).alias("max_unit_price_bucket_so_far"),
        F.dense_rank().over(w_operator_by_unit_price_ties).alias("unit_price_dense_rank"),
        F.sum("unit_price").over(w_operator_by_unit_price_movin).alias("unit_price_sum_2"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type), 2).alias("unit_price_vs_move_type_avg"),
        F.last("unit_price_bucket").over(w_sku_id_by_unit_price_bucket).alias("last_unit_price_bucket"),
    )
)

first_rows = (
    with_windows
    .filter((F.col("unit_price_dense_rank") <= 5) & (F.month(F.col("moved_at_week")).isin(2, 3)))
    .drop("unit_price_dense_rank")
)

stats = (
    first_rows
    .filter(F.col("unit_price_share_of_operator_move_type") <= 0.6)
    .groupBy("move_type", "unit_price_decile", "warehouse_id")
    .agg(
        F.countDistinct("operator").alias("distinct_operators"),
        F.sum("unit_price").alias("total_unit_price"),
        F.round(100 * F.count(F.when(F.col("move_type").isin("TRANSFER", "ADJUST", "OUTBOUND"), 1)) / F.count("*"), 1).alias("pct_move_type_in_transfer_adjust"),
        F.percentile_approx("unit_price_share_of_operator_move_type", 0.5).alias("median_unit_price_share_of_operator_move_type"),
        F.round(100 * F.count(F.when(F.col("move_type") == "ADJUST", 1)) / F.count("*"), 1).alias("pct_move_type_adjust"),
    )
)

result = (
    stats
    .select(
        "move_type",
        "unit_price_decile",
        "warehouse_id",
        "distinct_operators",
        "total_unit_price",
        "pct_move_type_in_transfer_adjust",
        "median_unit_price_share_of_operator_move_type",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
    .orderBy(
        F.col("unit_price_decile").asc_nulls_last(),
        F.col("move_type_len").asc_nulls_last(),
    )
)
