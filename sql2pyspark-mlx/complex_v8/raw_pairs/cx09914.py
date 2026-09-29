from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_moved_at_month_by = Window.partitionBy("move_type", F.date_trunc("month", "moved_at")).orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_all_by_unit_price_ties = Window.orderBy("unit_price")
w_operator = Window.partitionBy("operator")
w_operator_by_qty_ties = Window.partitionBy("operator").orderBy(F.col("qty").desc())
w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_unit_price_rank_distinct_sku = Window.partitionBy("unit_price_rank", "distinct_sku_id_in_operator")
w_operator_distinct_sku_id_in = Window.partitionBy("operator", "distinct_sku_id_in_operator").orderBy(F.col("moved_at_week").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_unit_price_rank_by_prev_unit = Window.partitionBy("unit_price_rank").orderBy("prev_unit_price", "id").rowsBetween(-6, -1)
w_operator_distinct_sku_id_in_2 = Window.partitionBy("operator", "distinct_sku_id_in_operator").orderBy(F.col("moved_at_week").desc(), "id")
w_warehouse_id_by_unit_price_r = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price_rank").desc(), "id")

recent_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

metrics = (
    recent_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "operator_clean",
        F.rank().over(w_move_type_moved_at_month_by).alias("moved_at_rank"),
        F.count(F.lit(1)).over(w_sku_id_move_type).alias("rows_in_sku_id_move_type"),
        F.rank().over(w_all_by_unit_price_ties).alias("unit_price_rank"),
        F.max("unit_price").over(w_sku_id_move_type).alias("sku_id_move_type_max_unit_price"),
        F.lag("unit_price").over(w_move_type_moved_at_month_by).alias("prev_unit_price"),
        F.size(F.collect_set("sku_id").over(w_operator)).alias("distinct_sku_id_in_operator"),
        F.rank().over(w_operator_by_qty_ties).alias("qty_rank"),
        F.sum("qty").over(w_all_by_unit_price_running).alias("running_qty"),
    )
)

scored = (
    metrics
    .filter(F.col("distinct_sku_id_in_operator") <= 9)
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator",
        "operator_clean",
        "moved_at_rank",
        "rows_in_sku_id_move_type",
        "unit_price_rank",
        "prev_unit_price",
        "distinct_sku_id_in_operator",
        "running_qty",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
    )
)

ranked = (
    scored
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator_clean",
        "moved_at_rank",
        "rows_in_sku_id_move_type",
        "unit_price_rank",
        "prev_unit_price",
        "distinct_sku_id_in_operator",
        "running_qty",
        "moved_at_week",
        F.round(F.avg("prev_unit_price").over(w_unit_price_rank_distinct_sku), 2).alias("unit_price_rank_distinct_sku_id_in_operator_avg_"),
        F.first("prev_unit_price", ignorenulls=True).over(w_operator_distinct_sku_id_in).alias("first_known_prev_unit_price"),
        F.sum("rows_in_sku_id_move_type").over(w_unit_price_rank_by_prev_unit).alias("rows_in_sku_id_move_type_sum_6_prior"),
        F.lag("moved_at_week", 3).over(w_operator_distinct_sku_id_in_2).alias("prev3_moved_at_week"),
        F.round(F.col("moved_at_rank") - F.avg("moved_at_rank").over(w_unit_price_rank_distinct_sku), 2).alias("moved_at_rank_vs_unit_price_rank_distinct_sku_id"),
        F.lead("moved_at_week").over(w_warehouse_id_by_unit_price_r).alias("next_moved_at_week"),
        F.ntile(5).over(w_warehouse_id_by_unit_price_r).alias("unit_price_rank_quintile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator_clean",
        "moved_at_rank",
        "rows_in_sku_id_move_type",
        "unit_price_rank",
        "prev_unit_price",
        "running_qty",
        "unit_price_rank_distinct_sku_id_in_operator_avg_",
        "first_known_prev_unit_price",
        "rows_in_sku_id_move_type_sum_6_prior",
        "prev3_moved_at_week",
        "moved_at_rank_vs_unit_price_rank_distinct_sku_id",
        "unit_price_rank_quintile",
        F.concat_ws("-", F.col("operator_clean"), F.col("move_type")).alias("operator_clean_move_type_label"),
    )
    .orderBy(F.col("rows_in_sku_id_move_type").desc())
)
