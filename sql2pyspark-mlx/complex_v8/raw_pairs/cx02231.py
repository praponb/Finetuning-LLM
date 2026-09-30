from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_sku_id = Window.partitionBy("move_type", "sku_id")
w_moved_at_month_by_days_since = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy("days_since_moved_at")
w_all_by_days_since_moved_at_m = Window.orderBy(F.col("days_since_moved_at").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_days_since_moved_at = Window.orderBy(F.col("days_since_moved_at").desc_nulls_first(), "id")
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_move_type_or_unknown_warehou = Window.partitionBy("move_type_or_unknown", "warehouse_id").orderBy("move_type_sku_id_sum_days_since_moved_at", "id")
w_move_type_move_type_or_unkno = Window.partitionBy("move_type", "move_type_or_unknown")
w_sku_id_by_days_since_moved_a = Window.partitionBy("sku_id").orderBy(F.col("days_since_moved_at_vs_move_type_sku_id_avg").desc(), "id")
w_move_type = Window.partitionBy("move_type")
w_warehouse_id_sku_id_by_days = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("days_since_moved_at_sum_3").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_days_since_moved_a_2 = Window.partitionBy("sku_id").orderBy(F.col("days_since_moved_at_vs_move_type_sku_id_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_moves = (
    df_stock_moves
    .filter(F.col("qty") >= 78)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

with_trends = (
    base_moves
    .select(
        "*",
        F.sum("days_since_moved_at").over(w_move_type_sku_id).alias("move_type_sku_id_sum_days_since_moved_at"),
        F.size(F.collect_set("warehouse_id").over(w_move_type_sku_id)).alias("distinct_warehouse_id_in_move_type_sku_id"),
        F.rank().over(w_moved_at_month_by_days_since).alias("days_since_moved_at_rank"),
        F.count(F.lit(1)).over(w_move_type_sku_id).alias("rows_in_move_type_sku_id"),
        F.sum("days_since_moved_at").over(w_all_by_days_since_moved_at_m).alias("days_since_moved_at_sum_3"),
        F.round(F.col("days_since_moved_at") - F.avg("days_since_moved_at").over(w_move_type_sku_id), 2).alias("days_since_moved_at_vs_move_type_sku_id_avg"),
        F.lead("unit_price", 2).over(w_all_by_days_since_moved_at).alias("next2_unit_price"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type_sku_id), 2).alias("unit_price_vs_move_type_sku_id_avg"),
        F.row_number().over(w_all_by_moved_at).alias("rn"),
    )
)

derived = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price",
        "move_type_sku_id_sum_days_since_moved_at",
        "rows_in_move_type_sku_id",
        "days_since_moved_at_sum_3",
        "days_since_moved_at_vs_move_type_sku_id_avg",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

ranked = (
    derived
    .select(
        "*",
        (F.col("days_since_moved_at_sum_3") - F.lag("days_since_moved_at_sum_3").over(w_move_type_or_unknown_warehou)).alias("days_since_moved_at_sum_3_change"),
        F.min("rows_in_move_type_sku_id").over(w_move_type_move_type_or_unkno).alias("move_type_move_type_or_unknown_min_rows_in_move_"),
        F.ntile(4).over(w_sku_id_by_days_since_moved_a).alias("days_since_moved_at_vs_move_type_sku_id_avg_quar"),
        F.round(F.col("rows_in_move_type_sku_id") - F.avg("rows_in_move_type_sku_id").over(w_move_type), 2).alias("rows_in_move_type_sku_id_vs_move_type_avg"),
        F.last("days_since_moved_at_sum_3", ignorenulls=True).over(w_warehouse_id_sku_id_by_days).alias("last_known_days_since_moved_at_sum_3"),
        F.nth_value("rows_in_move_type_sku_id", 2).over(w_sku_id_by_days_since_moved_a_2).alias("second_rows_in_move_type_sku_id"),
        F.max("unit_price").over(w_move_type_move_type_or_unkno).alias("move_type_move_type_or_unknown_max_unit_price"),
    )
)

stats = (
    ranked
    .groupBy(
        "days_since_moved_at_vs_move_type_sku_id_avg_quar",
        "sku_id",
        "warehouse_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.round(100 * F.count(F.when(F.col("move_type").isin("INBOUND", "OUTBOUND", "TRANSFER"), 1)) / F.count("*"), 1).alias("pct_move_type_in_inbound_outboun"),
        F.sum("days_since_moved_at_vs_move_type_sku_id_avg_quar").alias("total_days_since_moved_at_vs_move_type_sku_id_av"),
    )
)

result = (
    stats
    .filter(F.col("pct_move_type_in_inbound_outboun") > 10.0)
    .select(
        "days_since_moved_at_vs_move_type_sku_id_avg_quar",
        "sku_id",
        "cnt",
        "total_days_since_moved_at_vs_move_type_sku_id_av",
        F.when(F.col("distinct_sku_ids") >= 6, "HIGH").when(F.col("distinct_sku_ids") >= 3, "MEDIUM").otherwise("LOW").alias("distinct_sku_ids_tier"),
    )
    .orderBy(
        F.col("total_days_since_moved_at_vs_move_type_sku_id_av").asc_nulls_last(),
    )
)
