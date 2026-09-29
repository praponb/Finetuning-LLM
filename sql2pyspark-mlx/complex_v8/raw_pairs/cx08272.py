from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_qty_tier_by_moved_at = Window.partitionBy("sku_id", "qty_tier").orderBy("moved_at_plus_1d", "id")
w_qty_tier_by_moved_at_full = Window.partitionBy("qty_tier").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_qty_tier_by_moved_at_2 = Window.partitionBy("sku_id", "qty_tier").orderBy(F.col("moved_at").desc_nulls_first())
w_moved_at_plus_1d_month_by_mo = Window.partitionBy(F.date_trunc("month", "moved_at_plus_1d")).orderBy(F.col("moved_at").desc_nulls_first(), "id")
w_move_type_qty_tier = Window.partitionBy("move_type", "qty_tier")
w_qty_tier_by_moved_at_ties = Window.partitionBy("qty_tier").orderBy(F.col("moved_at").desc())
w_all_by_moved_at_plus_1d = Window.orderBy(F.col("moved_at_plus_1d").desc(), "id")
w_first_moved_at_plus_1d_month = Window.partitionBy(F.date_trunc("month", "first_moved_at_plus_1d"), "move_type")
w_moved_at_month = Window.partitionBy(F.month("moved_at"))
w_moved_at_dense_rank_sku_id_b = Window.partitionBy("moved_at_dense_rank", "sku_id").orderBy("prev_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(6, 12))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 1 DAYS")).alias("moved_at_plus_1d"),
        F.when(F.col("qty") >= 300, "large").when(F.col("qty") >= 111, "medium").otherwise("small").alias("qty_tier"),
    )
)

with_trends = (
    base
    .select(
        "*",
        F.lag("moved_at").over(w_sku_id_qty_tier_by_moved_at).alias("prev_moved_at"),
        F.first("moved_at_plus_1d").over(w_qty_tier_by_moved_at_full).alias("first_moved_at_plus_1d"),
        F.round(F.percent_rank().over(w_sku_id_qty_tier_by_moved_at_2), 3).alias("moved_at_percent_rank"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_moved_at_plus_1d_month_by_mo))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.round(F.percent_rank().over(w_sku_id_qty_tier_by_moved_at), 3).alias("moved_at_plus_1d_percent_rank"),
        F.min("unit_price").over(w_move_type_qty_tier).alias("move_type_qty_tier_min_unit_price"),
        F.dense_rank().over(w_qty_tier_by_moved_at_ties).alias("moved_at_dense_rank"),
        (F.col("unit_price") - F.lag("unit_price").over(w_moved_at_plus_1d_month_by_mo)).alias("unit_price_change"),
        F.round(F.cume_dist().over(w_sku_id_qty_tier_by_moved_at_2), 3).alias("moved_at_cume_dist"),
        F.size(F.collect_set("warehouse_id").over(w_move_type_qty_tier)).alias("distinct_warehouse_id_in_move_type_qty_tier"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.lead("prev_moved_at", 3).over(w_all_by_moved_at_plus_1d).alias("next3_prev_moved_at"),
        F.round(F.col("moved_at_cume_dist") / F.nullif(F.sum("moved_at_cume_dist").over(w_first_moved_at_plus_1d_month), F.lit(0)), 4).alias("moved_at_cume_dist_share_of_first_moved_at_plus_"),
        F.min("unit_price_change").over(w_moved_at_month).alias("moved_at_month_min_unit_price_change"),
        F.nth_value("moved_at", 2).over(w_moved_at_dense_rank_sku_id_b).alias("second_moved_at"),
        F.round(F.col("moved_at_cume_dist") - F.avg("moved_at_cume_dist").over(w_first_moved_at_plus_1d_month), 2).alias("moved_at_cume_dist_vs_first_moved_at_plus_1d_mon"),
        F.round(F.col("minutes_since_prev_moved_at") / F.nullif(F.sum("minutes_since_prev_moved_at").over(w_first_moved_at_plus_1d_month), F.lit(0)), 4).alias("minutes_since_prev_moved_at_share_of_first_moved"),
    )
)

result = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "unit_price",
        "moved_at_plus_1d",
        "qty_tier",
        "prev_moved_at",
        "first_moved_at_plus_1d",
        "moved_at_percent_rank",
        "minutes_since_prev_moved_at",
        "moved_at_plus_1d_percent_rank",
        "move_type_qty_tier_min_unit_price",
        "unit_price_change",
        "moved_at_cume_dist",
        "next3_prev_moved_at",
        "moved_at_month_min_unit_price_change",
        "moved_at_cume_dist_vs_first_moved_at_plus_1d_mon",
        "minutes_since_prev_moved_at_share_of_first_moved",
    )
    .orderBy(F.col("moved_at_plus_1d").desc())
)
