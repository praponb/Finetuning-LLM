from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id_by_moved = Window.partitionBy("sku_id", "warehouse_id").orderBy("moved_at")
w_sku_id_moved_at_month_by_mov = Window.partitionBy("sku_id", F.date_trunc("month", "moved_at")).orderBy(F.col("moved_at").desc(), "id")
w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_by_qty_ties = Window.partitionBy("warehouse_id").orderBy("qty")
w_sku_id_warehouse_id_by_moved_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_moved_at_month = Window.partitionBy("warehouse_id", F.month("moved_at"))
w_warehouse_id_by_moved_at_pri = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(-3, -1)
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id")
w_sku_id_moved_at_month_by_mov_2 = Window.partitionBy("sku_id", F.date_trunc("month", "moved_at")).orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_moved_at_tercile_fill = Window.orderBy("moved_at_tercile_filled", "id")
w_moved_at_tercile_filled = Window.partitionBy("moved_at_tercile_filled")
w_all_by_moved_at_tercile_fill_2 = Window.orderBy("moved_at_tercile_filled")
w_all_by_sku_id_max_sku_unit_c = Window.orderBy(F.col("sku_id_max_sku_unit_cost").desc())
w_all_by_moved_at_tercile_fill_3 = Window.orderBy("moved_at_tercile_filled", "id").rowsBetween(-2, -1)

recent_moves = (
    df_stock_moves
    .filter(F.col("sku_id") <= 17)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("operator_ref"),
    )
)

enriched_sku = (
    recent_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.operator_ref",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

metrics = (
    enriched_sku
    .select(
        "*",
        F.round(F.percent_rank().over(w_sku_id_warehouse_id_by_moved), 3).alias("moved_at_percent_rank"),
        F.ntile(3).over(w_sku_id_moved_at_month_by_mov).alias("moved_at_tercile"),
        F.sum("sku_unit_cost").over(w_sku_id).alias("sku_id_sum_sku_unit_cost"),
        F.round(F.cume_dist().over(w_warehouse_id_by_qty_ties), 3).alias("qty_cume_dist"),
        F.lag("unit_price", 2, 0).over(w_sku_id_moved_at_month_by_mov).alias("prev2_unit_price"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.round(F.cume_dist().over(w_sku_id_warehouse_id_by_moved_2), 3).alias("moved_at_cume_dist"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id_moved_at_month), 2).alias("qty_vs_warehouse_id_moved_at_month_avg"),
        F.round(F.avg("sku_unit_cost").over(w_warehouse_id_by_moved_at_pri), 2).alias("sku_unit_cost_avg_3_prior"),
        F.lead("qty").over(w_sku_id_by_moved_at).alias("next_qty"),
        F.max("unit_price").over(w_sku_id_moved_at_month_by_mov_2).alias("max_unit_price_so_far"),
        F.round(F.col("sku_unit_cost") / F.nullif(F.sum("sku_unit_cost").over(w_warehouse_id_moved_at_month), F.lit(0)), 4).alias("sku_unit_cost_share_of_warehouse_id_moved_at_mon"),
        F.max("sku_unit_cost").over(w_sku_id).alias("sku_id_max_sku_unit_cost"),
    )
)

scored = (
    metrics
    .filter(F.col("sku_id_sum_sku_unit_cost") < 2800.3)
    .select(
        "*",
        F.coalesce(F.col("moved_at_tercile"), F.lit(0)).alias("moved_at_tercile_filled"),
        F.when(F.col("unit_price") >= 299.1, "HIGH").when(F.col("unit_price") >= 189.4, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
        F.coalesce(F.col("moved_at_cume_dist"), F.lit(0.0)).alias("moved_at_cume_dist_filled"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.lag("moved_at_tercile_filled").over(w_all_by_moved_at_tercile_fill).alias("prev_moved_at_tercile_filled"),
        F.size(F.collect_set("warehouse_id").over(w_moved_at_tercile_filled)).alias("distinct_warehouse_id_in_moved_at_tercile_filled"),
        F.round(F.percent_rank().over(w_all_by_moved_at_tercile_fill_2), 3).alias("moved_at_tercile_filled_percent_rank"),
        F.rank().over(w_all_by_sku_id_max_sku_unit_c).alias("sku_id_max_sku_unit_cost_rank"),
        F.round(F.avg("sku_id_sum_sku_unit_cost").over(w_all_by_moved_at_tercile_fill_3), 2).alias("sku_id_sum_sku_unit_cost_avg_2_prior"),
        F.round(F.col("moved_at_percent_rank") / F.nullif(F.sum("moved_at_percent_rank").over(w_moved_at_tercile_filled), F.lit(0)), 4).alias("moved_at_percent_rank_share_of_moved_at_tercile_"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "sku_unit_cost",
        "moved_at_tercile",
        "sku_id_sum_sku_unit_cost",
        "qty_cume_dist",
        "rows_in_sku_id",
        "qty_vs_warehouse_id_moved_at_month_avg",
        "sku_unit_cost_avg_3_prior",
        "next_qty",
        "sku_unit_cost_share_of_warehouse_id_moved_at_mon",
        "sku_id_max_sku_unit_cost",
        "moved_at_tercile_filled",
        "moved_at_cume_dist_filled",
        "prev_moved_at_tercile_filled",
        "distinct_warehouse_id_in_moved_at_tercile_filled",
        "moved_at_tercile_filled_percent_rank",
        "sku_id_max_sku_unit_cost_rank",
        "sku_id_sum_sku_unit_cost_avg_2_prior",
        "moved_at_percent_rank_share_of_moved_at_tercile_",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
    .orderBy(
        F.col("qty_cume_dist").desc(),
        F.col("sku_unit_cost_share_of_warehouse_id_moved_at_mon").desc(),
    )
)
