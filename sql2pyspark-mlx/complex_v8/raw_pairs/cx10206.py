from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_warehouse_id_moved_at_year_m = Window.partitionBy("warehouse_id", F.date_trunc("month", "moved_at_year"))
w_moved_at_year_month_by_moved = Window.partitionBy(F.month("moved_at_year")).orderBy(F.col("moved_at_month").desc(), "id")
w_moved_at_year_to_date_by_mov = Window.partitionBy(F.to_date("moved_at_year")).orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_operator_by_mov = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("moved_at_month").asc_nulls_last())
w_move_type = Window.partitionBy("move_type")
w_all_by_moved_at_year_ties = Window.orderBy(F.col("moved_at_year").desc())
w_all_by_moved_at_year_prior_3 = Window.orderBy(F.col("moved_at_year").desc(), "id").rowsBetween(-3, -1)
w_warehouse_id_moved_at_year_m_2 = Window.partitionBy("warehouse_id", F.month("moved_at_year"))
w_all_by_moved_at_year_running = Window.orderBy("moved_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_year_moved_at_month = Window.partitionBy(F.year("moved_at"), "moved_at_month_rank").orderBy(F.col("distinct_sku_id_in_warehouse_id_moved_at_year_mo").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_moves = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.month(F.col("moved_at")).alias("moved_at_month"),
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
    )
)

windowed = (
    filtered_moves
    .select(
        "*",
        F.min("unit_price").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_min_unit_price"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_moved_at_year_m), 2).alias("unit_price_vs_warehouse_id_moved_at_year_month_a"),
        F.rank().over(w_moved_at_year_month_by_moved).alias("moved_at_month_rank"),
        F.size(F.collect_set("move_type").over(w_warehouse_id_moved_at_year_m)).alias("distinct_move_type_in_warehouse_id_moved_at_year"),
        F.first("unit_price").over(w_moved_at_year_to_date_by_mov).alias("first_unit_price"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id_moved_at_year_m)).alias("distinct_sku_id_in_warehouse_id_moved_at_year_mo"),
        F.round(F.percent_rank().over(w_warehouse_id_operator_by_mov), 3).alias("moved_at_month_percent_rank"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_move_type), F.lit(0)), 4).alias("qty_share_of_move_type"),
        F.max("unit_price").over(w_move_type).alias("move_type_max_unit_price"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_moved_at_year_ties), 3).alias("moved_at_year_percent_rank"),
        F.round(F.avg("qty").over(w_all_by_moved_at_year_prior_3), 2).alias("qty_avg_3_prior"),
        F.round(F.col("move_type_max_unit_price") - F.avg("move_type_max_unit_price").over(w_warehouse_id_moved_at_year_m_2), 2).alias("move_type_max_unit_price_vs_warehouse_id_moved_a"),
        F.max("move_type_max_unit_price").over(w_all_by_moved_at_year_prior_3).alias("move_type_max_unit_price_max_3_prior"),
        F.max("moved_at_month").over(w_all_by_moved_at_year_running).alias("max_moved_at_month_so_far"),
        F.round(F.col("moved_at_month_rank") - F.avg("moved_at_month_rank").over(w_warehouse_id_moved_at_year_m_2), 2).alias("moved_at_month_rank_vs_warehouse_id_moved_at_yea"),
        F.first("unit_price", ignorenulls=True).over(w_moved_at_year_moved_at_month).alias("first_known_unit_price"),
    )
)

totals = (
    ranked
    .groupBy(
        "distinct_sku_id_in_warehouse_id_moved_at_year_mo",
        F.date_format("moved_at", "yyyy-MM").alias("moved_at_ym"),
        F.to_date("moved_at_year").alias("moved_at_year_day"),
    )
    .agg(
        F.round(F.avg("moved_at_month"), 2).alias("avg_moved_at_month"),
        F.round(F.sum("moved_at_month_rank_vs_warehouse_id_moved_at_yea") / F.nullif(F.sum("move_type_max_unit_price_vs_warehouse_id_moved_a"), F.lit(0)), 3).alias("moved_at_month_rank_vs_warehouse_id_moved_at_yea_2"),
    )
)

result = (
    totals
    .select(
        "distinct_sku_id_in_warehouse_id_moved_at_year_mo",
        "moved_at_ym",
        "moved_at_month_rank_vs_warehouse_id_moved_at_yea_2",
        F.date_add(F.col("moved_at_year_day"), 365).alias("moved_at_year_day_plus_365d"),
        (F.floor(F.col("avg_moved_at_month") / 2) * 2).alias("avg_moved_at_month_bucket"),
    )
)
