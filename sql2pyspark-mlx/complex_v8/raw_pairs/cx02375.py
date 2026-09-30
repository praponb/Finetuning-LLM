from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month = Window.partitionBy(F.date_trunc("month", "moved_at"))
w_sku_id_by_moved_at_ties = Window.partitionBy("sku_id").orderBy("moved_at")
w_sku_id_warehouse_city_by_war = Window.partitionBy("sku_id", "warehouse_city").orderBy(F.col("warehouse_capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_warehouse_city_by_war_2 = Window.partitionBy("sku_id", "warehouse_city").orderBy(F.col("warehouse_capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_prior_2 = Window.orderBy("moved_at", "id").rowsBetween(-2, -1)
w_move_type = Window.partitionBy("move_type")
w_all_by_warehouse_capacity_fu = Window.orderBy(F.col("warehouse_capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at_moving_6 = Window.orderBy("moved_at", "id").rowsBetween(-6, Window.currentRow)
w_distinct_move_type_in_moved = Window.partitionBy("distinct_move_type_in_moved_at_month", "distinct_warehouse_id_in_moved_at_month").orderBy(F.col("distinct_warehouse_id_in_moved_at_month").desc_nulls_first())
w_distinct_move_type_in_moved_2 = Window.partitionBy("distinct_move_type_in_moved_at_month", "warehouse_id")
w_warehouse_id_by_distinct_mov = Window.partitionBy("warehouse_id").orderBy("distinct_move_type_in_moved_at_month", "id").rowsBetween(-3, -1)
w_distinct_move_type_in_moved_3 = Window.partitionBy("distinct_move_type_in_moved_at_month", "distinct_warehouse_id_in_moved_at_month").orderBy(F.col("distinct_warehouse_id_in_moved_at_month").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_warehouse_id_by_distinct_mov_2 = Window.partitionBy("warehouse_id").orderBy("distinct_move_type_in_moved_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_stock_moves
    .filter(F.col("unit_price") <= 372.6)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "warehouse_wh_code",
        "warehouse_capacity",
        F.size(F.collect_set("move_type").over(w_moved_at_month)).alias("distinct_move_type_in_moved_at_month"),
        F.round(F.cume_dist().over(w_sku_id_by_moved_at_ties), 3).alias("moved_at_cume_dist"),
        F.size(F.collect_set("warehouse_id").over(w_moved_at_month)).alias("distinct_warehouse_id_in_moved_at_month"),
        F.last("moved_at").over(w_sku_id_warehouse_city_by_war).alias("last_moved_at"),
        F.sum("warehouse_capacity").over(w_sku_id_warehouse_city_by_war_2).alias("running_warehouse_capacity"),
        F.round(F.avg("warehouse_capacity").over(w_all_by_moved_at_prior_2), 2).alias("warehouse_capacity_avg_2_prior"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_move_type), 2).alias("warehouse_capacity_vs_move_type_avg"),
        F.nth_value("warehouse_capacity", 2).over(w_all_by_warehouse_capacity_fu).alias("second_warehouse_capacity"),
        F.round(F.avg("warehouse_capacity").over(w_all_by_moved_at_moving_6), 2).alias("warehouse_capacity_avg_7"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.dense_rank().over(w_distinct_move_type_in_moved).alias("distinct_warehouse_id_in_moved_at_month_dense_ra"),
        F.count(F.lit(1)).over(w_distinct_move_type_in_moved_2).alias("rows_in_distinct_move_type_in_moved_at_month_war"),
        F.round(F.avg("second_warehouse_capacity").over(w_warehouse_id_by_distinct_mov), 2).alias("second_warehouse_capacity_avg_3_prior"),
        F.round(F.avg("warehouse_capacity_avg_7").over(w_distinct_move_type_in_moved_3), 2).alias("warehouse_capacity_avg_7_avg_3"),
        F.min("running_warehouse_capacity").over(w_distinct_move_type_in_moved_2).alias("distinct_move_type_in_moved_at_month_warehouse_i"),
        F.round(F.col("second_warehouse_capacity") / F.nullif(F.sum("second_warehouse_capacity").over(w_distinct_move_type_in_moved_2), F.lit(0)), 4).alias("second_warehouse_capacity_share_of_distinct_move"),
        F.sum("second_warehouse_capacity").over(w_warehouse_id_by_distinct_mov_2).alias("running_second_warehouse_capacity"),
    )
)

result = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "warehouse_wh_code",
        "warehouse_capacity",
        "distinct_move_type_in_moved_at_month",
        "moved_at_cume_dist",
        "distinct_warehouse_id_in_moved_at_month",
        "last_moved_at",
        "warehouse_capacity_vs_move_type_avg",
        "second_warehouse_capacity",
        "rows_in_distinct_move_type_in_moved_at_month_war",
        "second_warehouse_capacity_avg_3_prior",
        "warehouse_capacity_avg_7_avg_3",
        "distinct_move_type_in_moved_at_month_warehouse_i",
        "running_second_warehouse_capacity",
        F.length(F.trim(F.col("warehouse_wh_code"))).alias("warehouse_wh_code_len"),
        F.when(F.col("warehouse_capacity") >= 11005, "large").when(F.col("warehouse_capacity") >= 4912, "medium").otherwise("small").alias("warehouse_capacity_tier"),
    )
    .orderBy(F.col("id").desc())
)
