from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_warehouse_id_moved_at_hour_m = Window.partitionBy("warehouse_id", F.month("moved_at_hour")).orderBy("moved_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_moved_at_hour_m_2 = Window.partitionBy("warehouse_id", F.month("moved_at_hour")).orderBy("moved_at_hour", "id")
w_all_by_qty_ties = Window.orderBy("qty")
w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at_hour").desc(), "id").rowsBetween(-2, Window.currentRow)
w_warehouse_id_sku_id_by_moved_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at_hour").desc(), "id").rowsBetween(-2, -1)
w_warehouse_id_sku_id_by_moved_3 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_distinct_operator_in = Window.partitionBy("sku_id", "distinct_operator_in_sku_id_warehouse_id")
w_row_count_sku_id_by_row_coun = Window.partitionBy("row_count", "sku_id").orderBy(F.col("row_count").desc(), "sku_id", "moved_at_hour_tercile", "distinct_operator_in_sku_id_warehouse_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count_sku_id_by_row_coun_2 = Window.partitionBy("row_count", "sku_id").orderBy(F.col("row_count").desc(), "sku_id", "moved_at_hour_tercile", "distinct_operator_in_sku_id_warehouse_id")
w_distinct_operator_in_sku_id = Window.partitionBy("distinct_operator_in_sku_id_warehouse_id").orderBy("row_count", "sku_id", "moved_at_hour_tercile", "distinct_operator_in_sku_id_warehouse_id")

cleaned_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
)

enriched_sku = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.sku_code").isNotNull())
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.move_type_prefix",
        "s.moved_at_hour",
        "sk.sku_code",
    )
)

ranked = (
    enriched_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type_prefix",
        F.min("qty").over(w_sku_id_warehouse_id).alias("sku_id_warehouse_id_min_qty"),
        F.count(F.lit(1)).over(w_sku_id_warehouse_id).alias("rows_in_sku_id_warehouse_id"),
        F.nth_value("qty", 2).over(w_warehouse_id_moved_at_hour_m).alias("second_qty"),
        F.size(F.collect_set("operator").over(w_sku_id_warehouse_id)).alias("distinct_operator_in_sku_id_warehouse_id"),
        F.ntile(3).over(w_warehouse_id_moved_at_hour_m_2).alias("moved_at_hour_tercile"),
        F.round(F.cume_dist().over(w_all_by_qty_ties), 3).alias("qty_cume_dist"),
        F.round(F.avg("unit_price").over(w_warehouse_id_sku_id_by_moved), 2).alias("unit_price_avg_3"),
        F.round(F.avg("qty").over(w_warehouse_id_sku_id_by_moved_2), 2).alias("qty_avg_2_prior"),
        F.sum("qty").over(w_warehouse_id_sku_id_by_moved_3).alias("running_qty"),
    )
)

stats = (
    ranked
    .groupBy(
        "sku_id",
        "moved_at_hour_tercile",
        "distinct_operator_in_sku_id_warehouse_id",
    )
    .agg(
        F.round(F.stddev("rows_in_sku_id_warehouse_id"), 2).alias("stddev_rows_in_sku_id_warehouse_id"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.percentile_approx("second_qty", 0.5).alias("median_second_qty"),
        F.count("*").alias("row_count"),
    )
)

with_windows = (
    stats
    .select(
        "*",
        F.count(F.lit(1)).over(w_sku_id_distinct_operator_in).alias("rows_in_sku_id_distinct_operator_in_sku_id_wareh"),
        F.last("moved_at_hour_tercile", ignorenulls=True).over(w_row_count_sku_id_by_row_coun).alias("last_known_moved_at_hour_tercile"),
        F.ntile(4).over(w_row_count_sku_id_by_row_coun_2).alias("row_count_quartile"),
        F.row_number().over(w_distinct_operator_in_sku_id).alias("row_count_seq"),
    )
)

result = (
    with_windows
    .select(
        "sku_id",
        "moved_at_hour_tercile",
        "median_second_qty",
        "row_count_quartile",
        "row_count_seq",
        F.coalesce(F.col("row_count_quartile"), F.lit(0)).alias("row_count_quartile_filled"),
    )
    .orderBy(F.col("row_count_quartile").desc(), F.col("sku_id").desc())
)
