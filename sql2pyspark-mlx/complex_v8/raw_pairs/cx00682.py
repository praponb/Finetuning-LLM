from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_warehouse_id_by_uni = Window.partitionBy("operator", "warehouse_id").orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w_warehouse_id_by_days_since_m = Window.partitionBy("warehouse_id").orderBy("days_since_moved_at")
w_operator_warehouse_id_by_uni_2 = Window.partitionBy("operator", "warehouse_id").orderBy("unit_price", "id")
w_warehouse_id_sku_id_by_days = Window.partitionBy("warehouse_id", "sku_id").orderBy("days_since_moved_at")
w_operator = Window.partitionBy("operator")
w_operator_moved_at_month = Window.partitionBy("operator", F.month("moved_at"))
w_sku_id_by_moved_at_running = Window.partitionBy("sku_id").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_to_date = Window.partitionBy(F.to_date("moved_at"))

prepared_moves = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

metrics = (
    prepared_moves
    .select(
        "*",
        F.max("unit_price").over(w_operator_warehouse_id_by_uni).alias("unit_price_max_4"),
        F.round(F.percent_rank().over(w_warehouse_id_by_days_since_m), 3).alias("days_since_moved_at_percent_rank"),
        F.lag("moved_at", 3).over(w_operator_warehouse_id_by_uni_2).alias("prev3_moved_at"),
        F.rank().over(w_warehouse_id_sku_id_by_days).alias("days_since_moved_at_rank"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
        F.max("unit_price").over(w_operator_moved_at_month).alias("operator_moved_at_month_max_unit_price"),
        F.size(F.collect_set("warehouse_id").over(w_operator_moved_at_month)).alias("distinct_warehouse_id_in_operator_moved_at_month"),
        F.first("unit_price", ignorenulls=True).over(w_sku_id_by_moved_at_running).alias("first_known_unit_price"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator), F.lit(0)), 4).alias("unit_price_share_of_operator"),
    )
)

with_trends = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price",
        "days_since_moved_at",
        "days_since_moved_at_percent_rank",
        "days_since_moved_at_rank",
        "operator_moved_at_month_max_unit_price",
        "first_known_unit_price",
        "unit_price_share_of_operator",
        F.round(F.col("days_since_moved_at_rank") - F.avg("days_since_moved_at_rank").over(w_moved_at_to_date), 2).alias("days_since_moved_at_rank_vs_moved_at_to_date_avg"),
        F.sum("first_known_unit_price").over(w_moved_at_to_date).alias("moved_at_to_date_sum_first_known_unit_price"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_moved_at_to_date), F.lit(0)), 4).alias("unit_price_share_of_moved_at_to_date"),
    )
)

rollup_base = (
    with_trends
    .filter(F.col("days_since_moved_at_rank") < 6)
    .groupBy(F.year("moved_at").alias("moved_at_year"), "days_since_moved_at_rank")
    .agg(
        F.round(F.sum("days_since_moved_at") / F.nullif(F.sum("days_since_moved_at_percent_rank"), F.lit(0)), 3).alias("days_since_moved_at_to_days_since_moved_at_perce"),
        F.round(F.sum("moved_at_to_date_sum_first_known_unit_price"), 2).alias("total_moved_at_to_date_sum_first_known_unit_pric"),
        F.min("moved_at").alias("first_moved_at"),
        F.round(F.avg("days_since_moved_at_rank_vs_moved_at_to_date_avg"), 2).alias("avg_days_since_moved_at_rank_vs_moved_at_to_date"),
    )
)

result = (
    rollup_base
    .select(
        "moved_at_year",
        "first_moved_at",
        F.coalesce(F.col("days_since_moved_at_rank"), F.lit(0)).alias("days_since_moved_at_rank_filled"),
    )
    .orderBy(
        F.col("days_since_moved_at_rank_filled").asc_nulls_last(),
        F.col("moved_at_year").asc_nulls_last(),
    )
)
