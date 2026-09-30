from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_city_by_moved_at_p = Window.partitionBy("warehouse_city").orderBy("moved_at_plus_1d", "id")
w_moved_at_plus_1d_month_move = Window.partitionBy(F.month("moved_at_plus_1d"), "move_type").orderBy(F.col("moved_at_plus_1d").desc())
w_warehouse_city = Window.partitionBy("warehouse_city")
w_operator_warehouse_city_by_m = Window.partitionBy("operator", "warehouse_city").orderBy(F.col("moved_at_plus_1d").desc())
w_moved_at_plus_1d_month_move_2 = Window.partitionBy(F.month("moved_at_plus_1d"), "move_type").orderBy(F.col("moved_at_plus_1d").desc(), "id")
w_operator_warehouse_city_by_m_2 = Window.partitionBy("operator", "warehouse_city").orderBy(F.col("moved_at_plus_1d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_plus_1d_month_move_3 = Window.partitionBy(F.month("moved_at_plus_1d"), "move_type").orderBy(F.col("moved_at_plus_1d").desc(), "id").rowsBetween(-6, -1)

scoped_moves = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(6, 12))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
        (F.col("moved_at") + F.expr("INTERVAL 1 DAYS")).alias("moved_at_plus_1d"),
    )
)

with_parent_warehouse = (
    scoped_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.city").isNotNull())
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.move_type_prefix",
        "s.moved_at_plus_1d",
        F.col("w.city").alias("warehouse_city"),
    )
)

with_trends = (
    with_parent_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "warehouse_city",
        F.ntile(5).over(w_warehouse_city_by_moved_at_p).alias("moved_at_plus_1d_quintile"),
        F.lead("moved_at_plus_1d").over(w_warehouse_city_by_moved_at_p).alias("next_moved_at_plus_1d"),
        F.round(F.percent_rank().over(w_moved_at_plus_1d_month_move), 3).alias("moved_at_plus_1d_percent_rank"),
        F.size(F.collect_set("move_type").over(w_warehouse_city)).alias("distinct_move_type_in_warehouse_city"),
        F.count(F.lit(1)).over(w_warehouse_city).alias("rows_in_warehouse_city"),
        F.rank().over(w_operator_warehouse_city_by_m).alias("moved_at_plus_1d_rank"),
        F.round((F.unix_timestamp("moved_at_plus_1d") - F.unix_timestamp(F.lag("moved_at_plus_1d").over(w_moved_at_plus_1d_month_move_2))) / 60, 1).alias("minutes_since_prev_moved_at_plus_1d"),
        F.round(F.avg("qty").over(w_warehouse_city), 2).alias("warehouse_city_avg_qty"),
        F.lag("moved_at_plus_1d", 2).over(w_moved_at_plus_1d_month_move_2).alias("prev2_moved_at_plus_1d"),
        F.size(F.collect_set("sku_id").over(w_warehouse_city)).alias("distinct_sku_id_in_warehouse_city"),
        F.nth_value("moved_at_plus_1d", 2).over(w_operator_warehouse_city_by_m_2).alias("second_moved_at_plus_1d"),
        F.round(F.avg("qty").over(w_moved_at_plus_1d_month_move_3), 2).alias("qty_avg_6_prior"),
    )
)

annotated = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator",
        "warehouse_city",
        "moved_at_plus_1d_quintile",
        "next_moved_at_plus_1d",
        "moved_at_plus_1d_percent_rank",
        "distinct_move_type_in_warehouse_city",
        "moved_at_plus_1d_rank",
        "minutes_since_prev_moved_at_plus_1d",
        "warehouse_city_avg_qty",
        "prev2_moved_at_plus_1d",
        F.greatest(F.col("rows_in_warehouse_city"), F.col("qty_avg_6_prior")).alias("greatest_rows_in_warehouse_city_qty_avg_6_prior"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

result = (
    annotated
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "warehouse_city",
        "moved_at_plus_1d_quintile",
        "next_moved_at_plus_1d",
        "distinct_move_type_in_warehouse_city",
        "moved_at_plus_1d_rank",
        "warehouse_city_avg_qty",
        "prev2_moved_at_plus_1d",
        "greatest_rows_in_warehouse_city_qty_avg_6_prior",
        "move_type_slug",
    )
    .orderBy("warehouse_city_avg_qty", "id")
    .limit(10)
)
