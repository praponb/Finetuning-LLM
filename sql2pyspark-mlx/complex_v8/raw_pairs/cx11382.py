from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_sku_id = Window.partitionBy("sku_id")
w_sku_id_by_qty_running = Window.partitionBy("sku_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_days_since_moved_at_m = Window.orderBy("days_since_moved_at", "id").rowsBetween(-3, Window.currentRow)
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_all_by_days_since_moved_at = Window.orderBy("days_since_moved_at", "id")
w_unit_price_tercile = Window.partitionBy("unit_price_tercile")
w_sku_id_unit_price_rank_by_gr = Window.partitionBy("sku_id", "unit_price_rank").orderBy(F.col("greatest_unit_price_tercile_unit_price_rank").desc(), "id")
w_unit_price_tercile_by_qty_fu = Window.partitionBy("unit_price_tercile").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_stock_moves
    .filter((F.col("unit_price") < 396.4) | (F.hour(F.col("moved_at")).between(9, 14)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

sequenced = (
    recent
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at_label",
        "days_since_moved_at",
        F.size(F.collect_set("warehouse_id").over(w_sku_id_move_type)).alias("distinct_warehouse_id_in_sku_id_move_type"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id_move_type), 2).alias("qty_vs_sku_id_move_type_avg"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.last("days_since_moved_at", ignorenulls=True).over(w_sku_id_by_qty_running).alias("last_known_days_since_moved_at"),
        F.round(F.avg("unit_price").over(w_all_by_days_since_moved_at_m), 2).alias("unit_price_avg_4"),
        F.min("unit_price").over(w_sku_id_move_type).alias("sku_id_move_type_min_unit_price"),
        F.max("qty").over(w_sku_id_move_type).alias("sku_id_move_type_max_qty"),
        F.ntile(3).over(w_all_by_unit_price).alias("unit_price_tercile"),
        F.lead("unit_price", 1, 0).over(w_all_by_days_since_moved_at).alias("next_unit_price"),
        F.rank().over(w_all_by_unit_price).alias("unit_price_rank"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
    )
)

labeled = (
    sequenced
    .filter(F.col("moved_at_label").isNotNull())
    .select(
        "*",
        F.abs(F.col("next_unit_price") - 289.7).alias("next_unit_price_distance"),
        F.greatest(F.col("unit_price_tercile"), F.col("unit_price_rank")).alias("greatest_unit_price_tercile_unit_price_rank"),
        F.lower(F.trim(F.col("moved_at_label"))).alias("moved_at_label_clean"),
    )
)

sequenced_2 = (
    labeled
    .select(
        "*",
        F.max("days_since_moved_at").over(w_unit_price_tercile).alias("unit_price_tercile_max_days_since_moved_at"),
        F.lag("sku_id_move_type_max_qty").over(w_sku_id_unit_price_rank_by_gr).alias("prev_sku_id_move_type_max_qty"),
        F.first("next_unit_price").over(w_unit_price_tercile_by_qty_fu).alias("first_next_unit_price"),
        F.round(F.col("next_unit_price_distance") - F.avg("next_unit_price_distance").over(w_unit_price_tercile), 2).alias("next_unit_price_distance_vs_unit_price_tercile_a"),
        F.round(F.percent_rank().over(w_sku_id_unit_price_rank_by_gr), 3).alias("greatest_unit_price_tercile_unit_price_rank_perc"),
        F.sum("unit_price_tercile").over(w_unit_price_tercile).alias("unit_price_tercile_sum_unit_price_tercile"),
    )
)

result = (
    sequenced_2
    .filter(F.col("sku_id_move_type_min_unit_price").between(1500.7, 3250.3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at_label",
        "distinct_warehouse_id_in_sku_id_move_type",
        "unit_price_avg_4",
        "sku_id_move_type_max_qty",
        "unit_price_tercile",
        "next_unit_price",
        "unit_price_rank",
        "greatest_unit_price_tercile_unit_price_rank",
        "moved_at_label_clean",
        "unit_price_tercile_max_days_since_moved_at",
        "prev_sku_id_move_type_max_qty",
        "first_next_unit_price",
        "greatest_unit_price_tercile_unit_price_rank_perc",
        "unit_price_tercile_sum_unit_price_tercile",
    )
    .orderBy(F.col("first_next_unit_price").asc(), "unit_price_tercile")
)
