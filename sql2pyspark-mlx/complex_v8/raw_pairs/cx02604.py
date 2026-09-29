from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_sku_id_by_warehouse_opened_o = Window.partitionBy("sku_id").orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_qty_tier_warehouse_opened_on = Window.partitionBy("qty_tier", F.year("warehouse_opened_on")).orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_warehouse_opened_o_2 = Window.partitionBy("sku_id").orderBy(F.col("warehouse_opened_on").desc(), "id")
w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_city_move_type = Window.partitionBy("warehouse_city", "move_type")
w_qty_tier_warehouse_city_by_w = Window.partitionBy("qty_tier", "warehouse_city").orderBy(F.col("warehouse_opened_on").desc())
w_qty_tier_warehouse_opened_on_2 = Window.partitionBy("qty_tier", F.year("warehouse_opened_on")).orderBy(F.col("unit_price").desc(), "id")
w_qty_tier_warehouse_city_by_w_2 = Window.partitionBy("qty_tier", "warehouse_city").orderBy(F.col("warehouse_opened_on").desc(), "id")
w_warehouse_city_warehouse_ope = Window.partitionBy("warehouse_city", "warehouse_opened_on_quintile")
w_qty_tier_moved_at_year_by_ro = Window.partitionBy("qty_tier", F.year("moved_at")).orderBy(F.col("rows_in_warehouse_id_sku_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_moves = (
    df_stock_moves
    .filter((F.col("qty") > 137) & (F.col("unit_price").between(60.4, 317.0)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("qty") >= 392, "A").when(F.col("qty") >= 148, "B").otherwise("C").alias("qty_tier"),
    )
)

with_parent_warehouse = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

with_trends = (
    with_parent_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "qty_tier",
        "warehouse_wh_code",
        "warehouse_city",
        "warehouse_opened_on",
        F.round(F.avg("unit_price").over(w_warehouse_id_sku_id), 2).alias("warehouse_id_sku_id_avg_unit_price"),
        F.last("warehouse_opened_on").over(w_sku_id_by_warehouse_opened_o).alias("last_warehouse_opened_on"),
        F.first("moved_at", ignorenulls=True).over(w_qty_tier_warehouse_opened_on).alias("first_known_moved_at"),
        F.ntile(5).over(w_sku_id_by_warehouse_opened_o_2).alias("warehouse_opened_on_quintile"),
        F.count(F.lit(1)).over(w_warehouse_id_sku_id).alias("rows_in_warehouse_id_sku_id"),
        F.size(F.collect_set("qty_tier").over(w_warehouse_id_sku_id)).alias("distinct_qty_tier_in_warehouse_id_sku_id"),
        F.nth_value("qty", 2).over(w_all_by_moved_at_full).alias("second_qty"),
        F.count(F.lit(1)).over(w_warehouse_city_move_type).alias("rows_in_warehouse_city_move_type"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_city_move_type), F.lit(0)), 4).alias("qty_share_of_warehouse_city_move_type"),
        F.dense_rank().over(w_qty_tier_warehouse_city_by_w).alias("warehouse_opened_on_dense_rank"),
        F.max("qty").over(w_warehouse_city_move_type).alias("warehouse_city_move_type_max_qty"),
        F.nth_value("warehouse_opened_on", 2).over(w_all_by_moved_at_full).alias("second_warehouse_opened_on"),
        F.round(F.percent_rank().over(w_qty_tier_warehouse_opened_on_2), 3).alias("unit_price_percent_rank"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_qty_tier_warehouse_city_by_w_2))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.sum("minutes_since_prev_moved_at").over(w_warehouse_city_warehouse_ope).alias("warehouse_city_warehouse_opened_on_quintile_sum_"),
        F.count(F.lit(1)).over(w_qty_tier_moved_at_year_by_ro).alias("running_count"),
        F.count(F.lit(1)).over(w_warehouse_city_warehouse_ope).alias("rows_in_warehouse_city_warehouse_opened_on_quint"),
        F.min("warehouse_opened_on_quintile").over(w_warehouse_city_warehouse_ope).alias("warehouse_city_warehouse_opened_on_quintile_min_"),
    )
)

result = (
    metrics
    .filter(F.col("rows_in_warehouse_id_sku_id").between(5, 15))
    .select(
        "id",
        "moved_at",
        "unit_price",
        "qty_tier",
        "warehouse_wh_code",
        "last_warehouse_opened_on",
        "first_known_moved_at",
        "rows_in_warehouse_id_sku_id",
        "second_qty",
        "rows_in_warehouse_city_move_type",
        "qty_share_of_warehouse_city_move_type",
        "warehouse_opened_on_dense_rank",
        "second_warehouse_opened_on",
        "unit_price_percent_rank",
        "minutes_since_prev_moved_at",
        "warehouse_city_warehouse_opened_on_quintile_sum_",
        "running_count",
        "rows_in_warehouse_city_warehouse_opened_on_quint",
        "warehouse_city_warehouse_opened_on_quintile_min_",
    )
    .orderBy(F.col("warehouse_city_warehouse_opened_on_quintile_min_").desc())
)
