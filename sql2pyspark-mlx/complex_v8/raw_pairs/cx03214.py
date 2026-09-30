from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator_by_day = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("days_since_moved_at").desc())
w_warehouse_id_operator_by_day_2 = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("days_since_moved_at").desc(), "id")
w_operator_move_type = Window.partitionBy("operator", "move_type")
w_move_type_warehouse_id_by_un = Window.partitionBy("move_type", "warehouse_id").orderBy("unit_price_distance", "id")
w_move_type_warehouse_id_by_un_2 = Window.partitionBy("move_type", "warehouse_id").orderBy("unit_price_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_operator_by_day_3 = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("days_since_moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price_distance_r = Window.orderBy("unit_price_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_warehouse_id_in_ope = Window.partitionBy("distinct_warehouse_id_in_operator_move_type", "operator").orderBy(F.col("operator_move_type_sum_days_since_moved_at").desc(), "id")
w_sku_id = Window.partitionBy("sku_id")
w_distinct_warehouse_id_in_ope_2 = Window.partitionBy("distinct_warehouse_id_in_operator_move_type", "operator").orderBy(F.col("last_unit_price_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_stock_moves
    .filter(F.col("move_type") != "OUTBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.abs(F.col("unit_price") - 272.4).alias("unit_price_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price_distance",
        "days_since_moved_at",
        F.round(F.cume_dist().over(w_warehouse_id_operator_by_day), 3).alias("days_since_moved_at_cume_dist"),
        F.lead("unit_price_distance", 2).over(w_warehouse_id_operator_by_day_2).alias("next2_unit_price_distance"),
        F.size(F.collect_set("warehouse_id").over(w_operator_move_type)).alias("distinct_warehouse_id_in_operator_move_type"),
        F.count(F.lit(1)).over(w_operator_move_type).alias("rows_in_operator_move_type"),
        F.sum("days_since_moved_at").over(w_operator_move_type).alias("operator_move_type_sum_days_since_moved_at"),
        (F.col("days_since_moved_at") - F.lag("days_since_moved_at").over(w_move_type_warehouse_id_by_un)).alias("days_since_moved_at_change"),
        F.size(F.collect_set("sku_id").over(w_operator_move_type)).alias("distinct_sku_id_in_operator_move_type"),
        F.last("unit_price_distance").over(w_move_type_warehouse_id_by_un_2).alias("last_unit_price_distance"),
        F.max("qty").over(w_operator_move_type).alias("operator_move_type_max_qty"),
        F.sum("unit_price_distance").over(w_warehouse_id_operator_by_day_3).alias("running_unit_price_distance"),
        F.first("unit_price_distance", ignorenulls=True).over(w_all_by_unit_price_distance_r).alias("first_known_unit_price_distance"),
        F.sum("qty").over(w_warehouse_id_operator_by_day_3).alias("running_qty"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.row_number().over(w_distinct_warehouse_id_in_ope).alias("operator_move_type_sum_days_since_moved_at_seq"),
        F.round(F.col("days_since_moved_at") / F.nullif(F.sum("days_since_moved_at").over(w_sku_id), F.lit(0)), 4).alias("days_since_moved_at_share_of_sku_id"),
        F.lead("operator_move_type_max_qty").over(w_distinct_warehouse_id_in_ope).alias("next_operator_move_type_max_qty"),
        F.nth_value("days_since_moved_at", 2).over(w_distinct_warehouse_id_in_ope_2).alias("second_days_since_moved_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price_distance",
        "days_since_moved_at",
        "next2_unit_price_distance",
        "rows_in_operator_move_type",
        "operator_move_type_sum_days_since_moved_at",
        "days_since_moved_at_change",
        "distinct_sku_id_in_operator_move_type",
        "last_unit_price_distance",
        "operator_move_type_max_qty",
        "running_unit_price_distance",
        "first_known_unit_price_distance",
        "running_qty",
        "operator_move_type_sum_days_since_moved_at_seq",
        "days_since_moved_at_share_of_sku_id",
        "next_operator_move_type_max_qty",
        "second_days_since_moved_at",
        F.round(F.col("last_unit_price_distance") / F.nullif(F.col("running_qty"), F.lit(0)), 3).alias("last_unit_price_distance_per_running_qty"),
    )
    .orderBy(
        F.col("distinct_sku_id_in_operator_move_type").desc_nulls_first(),
        F.col("days_since_moved_at_share_of_sku_id").desc(),
    )
)
