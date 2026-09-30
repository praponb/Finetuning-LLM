from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_warehouse_id_by_moved = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_sku_id_warehouse_id_by_moved_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-2, -1)
w_sku_id_warehouse_id_by_moved_3 = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_sku_id_warehouse_id_by_moved_4 = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_warehouse_id_by_moved_5 = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_days_since_moved_at_s = Window.orderBy("days_since_moved_at_sum_4", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price_avg_2_prio = Window.orderBy("unit_price_avg_2_prior", "id")
w_all_by_unit_price_avg_2_prio_2 = Window.orderBy("unit_price_avg_2_prior", "id").rowsBetween(-6, Window.currentRow)
w_all_by_days_since_moved_at_s_2 = Window.orderBy("days_since_moved_at_sum_4", "id")
w_all_by_days_since_moved_at_s_3 = Window.orderBy("days_since_moved_at_sum_4")
w_all_by_max_qty_so_far_avg_7 = Window.orderBy("max_qty_so_far_avg_7", "id")
w_all_by_max_qty_so_far_avg_7_2 = Window.orderBy("max_qty_so_far_avg_7")
w_all_by_max_qty_so_far_avg_7_3 = Window.orderBy("max_qty_so_far_avg_7", "id").rowsBetween(-2, -1)
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")

recent_moves = (
    df_stock_moves
    .filter((F.col("sku_id") <= 19) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

with_windows = (
    recent_moves
    .select(
        "id",
        "qty",
        "unit_price",
        "days_since_moved_at",
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id), F.lit(0)), 4).alias("qty_share_of_warehouse_id"),
        F.round(F.avg("days_since_moved_at").over(w_sku_id_warehouse_id_by_moved), 2).alias("days_since_moved_at_avg_7"),
        F.round(F.avg("unit_price").over(w_sku_id_warehouse_id_by_moved_2), 2).alias("unit_price_avg_2_prior"),
        F.sum("days_since_moved_at").over(w_sku_id_warehouse_id_by_moved_3).alias("days_since_moved_at_sum_4"),
        F.max("qty").over(w_sku_id_warehouse_id_by_moved_4).alias("max_qty_so_far"),
        F.max("qty").over(w_sku_id_warehouse_id_by_moved_5).alias("qty_max_2"),
    )
)

with_activity = (
    with_windows
    .crossJoin(with_windows.agg(F.avg("days_since_moved_at_avg_7").alias("overall_avg_days_since_moved_at_avg_7")))
    .filter(F.col("days_since_moved_at_avg_7") > F.col("overall_avg_days_since_moved_at_avg_7"))
    .drop("overall_avg_days_since_moved_at_avg_7")
)

scored = (
    with_activity
    .filter(F.col("unit_price") >= 215.4)
    .select(
        "id",
        "unit_price",
        "days_since_moved_at",
        "qty_share_of_warehouse_id",
        "days_since_moved_at_avg_7",
        "unit_price_avg_2_prior",
        "days_since_moved_at_sum_4",
        "max_qty_so_far",
        "qty_max_2",
        F.round(F.col("unit_price_avg_2_prior") / F.nullif(F.col("qty_max_2"), F.lit(0)), 3).alias("unit_price_avg_2_prior_per_qty_max_2"),
        (F.col("qty") - F.col("unit_price_avg_2_prior")).alias("qty_minus_unit_price_avg_2_prior"),
        (F.floor(F.col("qty_max_2") / 250) * 250).alias("qty_max_2_bucket"),
    )
)

sequenced = (
    scored
    .select(
        "id",
        "unit_price",
        "days_since_moved_at",
        "days_since_moved_at_avg_7",
        "unit_price_avg_2_prior",
        "days_since_moved_at_sum_4",
        "qty_max_2",
        "unit_price_avg_2_prior_per_qty_max_2",
        "qty_minus_unit_price_avg_2_prior",
        "qty_max_2_bucket",
        F.last("qty_max_2_bucket", ignorenulls=True).over(w_all_by_days_since_moved_at_s).alias("last_known_qty_max_2_bucket"),
        F.lead("max_qty_so_far", 2).over(w_all_by_unit_price_avg_2_prio).alias("next2_max_qty_so_far"),
        F.round(F.avg("max_qty_so_far").over(w_all_by_unit_price_avg_2_prio_2), 2).alias("max_qty_so_far_avg_7"),
        F.lead("days_since_moved_at_avg_7").over(w_all_by_days_since_moved_at_s_2).alias("next_days_since_moved_at_avg_7"),
        F.lag("max_qty_so_far").over(w_all_by_unit_price_avg_2_prio).alias("prev_max_qty_so_far"),
        F.round(F.cume_dist().over(w_all_by_days_since_moved_at_s_3), 3).alias("days_since_moved_at_sum_4_cume_dist"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "unit_price",
        "days_since_moved_at",
        "days_since_moved_at_avg_7",
        "unit_price_avg_2_prior",
        "days_since_moved_at_sum_4",
        "unit_price_avg_2_prior_per_qty_max_2",
        "qty_max_2_bucket",
        "next2_max_qty_so_far",
        "max_qty_so_far_avg_7",
        "next_days_since_moved_at_avg_7",
        "prev_max_qty_so_far",
        F.round(F.percent_rank().over(w_all_by_max_qty_so_far_avg_7), 3).alias("max_qty_so_far_avg_7_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_max_qty_so_far_avg_7_2), 3).alias("max_qty_so_far_avg_7_cume_dist"),
        F.round(F.avg("qty_max_2_bucket").over(w_all_by_max_qty_so_far_avg_7_3), 2).alias("qty_max_2_bucket_avg_2_prior"),
        (F.col("qty_max_2_bucket") - F.lag("qty_max_2_bucket").over(w_all_by_unit_price)).alias("qty_max_2_bucket_change"),
    )
)

derived = (
    sequenced_2
    .filter(F.col("unit_price") <= 387.1)
    .select(
        "*",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.round(F.col("days_since_moved_at") / F.nullif(F.col("next2_max_qty_so_far"), F.lit(0)), 3).alias("days_since_moved_at_per_next2_max_qty_so_far"),
        (F.col("unit_price") + F.col("days_since_moved_at")).alias("unit_price_plus_days_since_moved_at"),
    )
)

result = (
    derived
    .select(
        "id",
        "unit_price",
        "days_since_moved_at",
        "days_since_moved_at_avg_7",
        "unit_price_avg_2_prior",
        "unit_price_avg_2_prior_per_qty_max_2",
        "qty_max_2_bucket",
        "next2_max_qty_so_far",
        "max_qty_so_far_avg_7",
        "prev_max_qty_so_far",
        "max_qty_so_far_avg_7_percent_rank",
        "max_qty_so_far_avg_7_cume_dist",
        "qty_max_2_bucket_change",
        "unit_price_filled",
        "days_since_moved_at_per_next2_max_qty_so_far",
        F.abs(F.col("qty_max_2_bucket") - 676).alias("qty_max_2_bucket_distance"),
    )
    .orderBy(
        F.col("unit_price_avg_2_prior_per_qty_max_2").desc(),
        F.col("days_since_moved_at_avg_7").desc(),
    )
)
