from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_days_since_moved = Window.partitionBy("operator").orderBy(F.col("days_since_moved_at").desc(), "id").rowsBetween(-6, -1)
w_warehouse_id_by_days_since_m = Window.partitionBy("warehouse_id").orderBy(F.col("days_since_moved_at").desc())
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_all_by_days_since_moved_at_r = Window.orderBy(F.col("days_since_moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_days_since_moved_at = Window.orderBy(F.col("days_since_moved_at").asc_nulls_last(), "id")

recent = (
    df_stock_moves
    .filter(F.col("move_type").isin("OUTBOUND", "ADJUST", "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

enriched_warehouse = (
    recent.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select("s.*", F.col("w.wh_code").alias("warehouse_wh_code"))
)

with_trends = (
    enriched_warehouse
    .select(
        "*",
        F.sum("unit_price_bucket").over(w_operator_by_days_since_moved).alias("unit_price_bucket_sum_6_prior"),
        F.round(F.cume_dist().over(w_warehouse_id_by_days_since_m), 3).alias("days_since_moved_at_cume_dist"),
        F.size(F.collect_set("warehouse_id").over(w_move_type_operator)).alias("distinct_warehouse_id_in_move_type_operator"),
        F.size(F.collect_set("sku_id").over(w_move_type_operator)).alias("distinct_sku_id_in_move_type_operator"),
        F.count(F.lit(1)).over(w_move_type_operator).alias("rows_in_move_type_operator"),
        F.first("days_since_moved_at", ignorenulls=True).over(w_all_by_days_since_moved_at_r).alias("first_known_days_since_moved_at"),
        F.round(F.percent_rank().over(w_all_by_days_since_moved_at), 3).alias("days_since_moved_at_percent_rank"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type_operator), F.lit(0)), 4).alias("unit_price_share_of_move_type_operator"),
        F.round(F.col("unit_price_bucket") / F.nullif(F.sum("unit_price_bucket").over(w_move_type_operator), F.lit(0)), 4).alias("unit_price_bucket_share_of_move_type_operator"),
        F.round(F.col("days_since_moved_at") - F.avg("days_since_moved_at").over(w_move_type_operator), 2).alias("days_since_moved_at_vs_move_type_operator_avg"),
    )
)

annotated = (
    with_trends
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "days_since_moved_at",
        "unit_price_bucket",
        "warehouse_wh_code",
        "unit_price_bucket_sum_6_prior",
        "distinct_warehouse_id_in_move_type_operator",
        "distinct_sku_id_in_move_type_operator",
        "rows_in_move_type_operator",
        "unit_price_bucket_share_of_move_type_operator",
        F.abs(F.col("unit_price_bucket") - 300).alias("unit_price_bucket_distance"),
    )
)

grouped = (
    annotated
    .groupBy("move_type")
    .agg(
        F.sum("unit_price_bucket").alias("total_unit_price_bucket"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
    )
)

result = (
    grouped
    .select("move_type")
    .orderBy(F.col("move_type").asc())
    .limit(20)
)
