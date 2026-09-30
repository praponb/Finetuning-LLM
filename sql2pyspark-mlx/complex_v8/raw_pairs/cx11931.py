from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_moved_at_year = Window.partitionBy("warehouse_id", F.year("moved_at"))
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), "id")
w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_sku_id_by_moved_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy("moved_at", "id")
w_moved_at_month_warehouse_id = Window.partitionBy(F.month("moved_at"), "warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_sku_id_by_moved_3 = Window.partitionBy("warehouse_id", "sku_id").orderBy("moved_at")
w_rn_distance = Window.partitionBy("rn_distance")

recent_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
    )
)

ranked = (
    recent_moves
    .select(
        "*",
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_moved_at_year), 2).alias("unit_price_vs_warehouse_id_moved_at_year_avg"),
        F.ntile(10).over(w_warehouse_id_by_qty).alias("qty_decile"),
        F.nth_value("qty", 2).over(w_warehouse_id_sku_id_by_moved).alias("second_qty"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_warehouse_id_sku_id_by_moved_2))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.row_number().over(w_warehouse_id_sku_id_by_moved_2).alias("rn"),
        F.last("moved_at_week", ignorenulls=True).over(w_moved_at_month_warehouse_id).alias("last_known_moved_at_week"),
        F.first("unit_price").over(w_warehouse_id_sku_id_by_moved).alias("first_unit_price"),
        F.min("unit_price").over(w_warehouse_id_moved_at_year).alias("warehouse_id_moved_at_year_min_unit_price"),
        F.round(F.cume_dist().over(w_warehouse_id_sku_id_by_moved_3), 3).alias("moved_at_cume_dist"),
    )
)

tagged = (
    ranked
    .filter(F.col("minutes_since_prev_moved_at") <= 4250.0)
    .select(
        "*",
        F.to_date(F.col("last_known_moved_at_week")).alias("last_known_moved_at_week_date"),
        F.abs(F.col("rn") - 6).alias("rn_distance"),
        F.round(F.col("first_unit_price") / F.nullif(F.col("qty_decile"), F.lit(0)), 3).alias("first_unit_price_per_qty_decile"),
    )
)

metrics = (
    tagged
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price",
        "operator_move_type_label",
        "moved_at_week",
        "unit_price_vs_warehouse_id_moved_at_year_avg",
        "qty_decile",
        "second_qty",
        "minutes_since_prev_moved_at",
        "last_known_moved_at_week",
        "warehouse_id_moved_at_year_min_unit_price",
        "last_known_moved_at_week_date",
        "rn_distance",
        "first_unit_price_per_qty_decile",
        F.size(F.collect_set("warehouse_id").over(w_rn_distance)).alias("distinct_warehouse_id_in_rn_distance"),
        F.size(F.collect_set("sku_id").over(w_rn_distance)).alias("distinct_sku_id_in_rn_distance"),
        F.round(F.col("minutes_since_prev_moved_at") / F.nullif(F.sum("minutes_since_prev_moved_at").over(w_rn_distance), F.lit(0)), 4).alias("minutes_since_prev_moved_at_share_of_rn_distance"),
    )
)

result = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price",
        "moved_at_week",
        "second_qty",
        "minutes_since_prev_moved_at",
        "last_known_moved_at_week",
        "warehouse_id_moved_at_year_min_unit_price",
        "last_known_moved_at_week_date",
        "rn_distance",
        "first_unit_price_per_qty_decile",
        "distinct_warehouse_id_in_rn_distance",
        "minutes_since_prev_moved_at_share_of_rn_distance",
    )
    .orderBy("last_known_moved_at_week", F.col("moved_at_week").asc())
)
