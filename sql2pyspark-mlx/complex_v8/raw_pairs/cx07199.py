from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_move_type = Window.partitionBy("operator", "move_type")
w_move_type_sku_id_by_unit_pri = Window.partitionBy("move_type", "sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_all_by_rows_in_operator_move = Window.orderBy("rows_in_operator_move_type", "id")
w_all_by_second_qty = Window.orderBy(F.col("second_qty").desc(), "id")
w_all_by_unit_price_moving_6 = Window.orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w_second_qty_quartile = Window.partitionBy("second_qty_quartile")
w_second_qty_quartile_by_disti = Window.partitionBy("second_qty_quartile").orderBy("distinct_ids")
w_all_by_second_qty_quartile_m = Window.orderBy(F.col("second_qty_quartile_max_distinct_ids").desc(), "distinct_warehouse_id_in_operator_move_type", "second_qty_quartile", "moved_at_day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_second_qty_quartile_m_2 = Window.orderBy(F.col("second_qty_quartile_max_distinct_ids").desc(), "distinct_warehouse_id_in_operator_move_type", "second_qty_quartile", "moved_at_day").rowsBetween(-6, -1)

prepared = (
    df_stock_moves
    .filter((F.col("move_type").isin("ADJUST", "TRANSFER")) & (F.col("warehouse_id") >= 5))
)

metrics = (
    prepared
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.count(F.lit(1)).over(w_operator_move_type).alias("rows_in_operator_move_type"),
        F.size(F.collect_set("warehouse_id").over(w_operator_move_type)).alias("distinct_warehouse_id_in_operator_move_type"),
        F.nth_value("qty", 2).over(w_move_type_sku_id_by_unit_pri).alias("second_qty"),
    )
)

without_matches = (
    metrics.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isNotNull()), "left_semi")
)

with_trends = (
    without_matches
    .select(
        "id",
        "moved_at",
        "distinct_warehouse_id_in_operator_move_type",
        F.count(F.lit(1)).over(w_operator_warehouse_id).alias("rows_in_operator_warehouse_id"),
        F.row_number().over(w_all_by_rows_in_operator_move).alias("seq_no"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_all_by_rows_in_operator_move))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.lag("qty", 3).over(w_all_by_rows_in_operator_move).alias("prev3_qty"),
        F.ntile(4).over(w_all_by_second_qty).alias("second_qty_quartile"),
        F.sum("qty").over(w_all_by_unit_price_moving_6).alias("qty_sum_7"),
    )
)

rollup_base = (
    with_trends
    .groupBy(
        "distinct_warehouse_id_in_operator_move_type",
        "second_qty_quartile",
        F.to_date("moved_at").alias("moved_at_day"),
    )
    .agg(
        F.round(F.avg("distinct_warehouse_id_in_operator_move_type"), 2).alias("avg_distinct_warehouse_id_in_operator_move_type"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

with_trends_2 = (
    rollup_base
    .select(
        "*",
        F.max("distinct_ids").over(w_second_qty_quartile).alias("second_qty_quartile_max_distinct_ids"),
        F.count(F.lit(1)).over(w_second_qty_quartile).alias("rows_in_second_qty_quartile"),
        F.round(F.percent_rank().over(w_second_qty_quartile_by_disti), 3).alias("distinct_ids_percent_rank"),
    )
)

metrics_2 = (
    with_trends_2
    .select(
        "distinct_warehouse_id_in_operator_move_type",
        "second_qty_quartile",
        "moved_at_day",
        "distinct_ids",
        "distinct_ids_percent_rank",
        F.first("moved_at_day", ignorenulls=True).over(w_all_by_second_qty_quartile_m).alias("first_known_moved_at_day"),
        F.round(F.avg("distinct_ids").over(w_all_by_second_qty_quartile_m_2), 2).alias("distinct_ids_avg_6_prior"),
    )
)

result = (
    metrics_2
    .select(
        "distinct_warehouse_id_in_operator_move_type",
        "second_qty_quartile",
        "distinct_ids_percent_rank",
        "first_known_moved_at_day",
        "distinct_ids_avg_6_prior",
        F.round(F.col("distinct_ids_avg_6_prior") * 1.1, 2).alias("distinct_ids_avg_6_prior_adj"),
    )
)
