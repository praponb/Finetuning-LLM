from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id")
w_move_type = Window.partitionBy("move_type")
w_qty_tier_move_type = Window.partitionBy("qty_tier", "move_type")
w_qty_tier_sku_id_by_moved_at = Window.partitionBy("qty_tier", "sku_id").orderBy("moved_at", "id")
w_warehouse_id_by_unit_price_t = Window.partitionBy("warehouse_id").orderBy("unit_price")
w_warehouse_id_qty_tier_by_mov = Window.partitionBy("warehouse_id", "qty_tier").orderBy("moved_at", "id").rowsBetween(-2, -1)
w_warehouse_id_qty_tier_by_mov_2 = Window.partitionBy("warehouse_id", "qty_tier").orderBy("moved_at", "id").rowsBetween(-1, -1)
w_warehouse_id_qty_tier_by_mov_3 = Window.partitionBy("warehouse_id", "qty_tier").orderBy("moved_at")
w_qty_tier_sku_id_by_moved_at_2 = Window.partitionBy("qty_tier", "sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_moves = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("qty") >= 349, "large").when(F.col("qty") >= 198, "medium").otherwise("small").alias("qty_tier"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

metrics = (
    base_moves
    .select(
        "*",
        F.lead("unit_price", 2).over(w_sku_id_by_moved_at).alias("next2_unit_price"),
        F.round(F.col("unit_price_plus_qty") - F.avg("unit_price_plus_qty").over(w_move_type), 2).alias("unit_price_plus_qty_vs_move_type_avg"),
        F.size(F.collect_set("operator").over(w_qty_tier_move_type)).alias("distinct_operator_in_qty_tier_move_type"),
        F.round(F.col("unit_price_plus_qty") - F.avg("unit_price_plus_qty").over(w_qty_tier_move_type), 2).alias("unit_price_plus_qty_vs_qty_tier_move_type_avg"),
        F.lead("unit_price").over(w_qty_tier_sku_id_by_moved_at).alias("next_unit_price"),
        F.rank().over(w_warehouse_id_by_unit_price_t).alias("unit_price_rank"),
        F.round(F.avg("unit_price_plus_qty").over(w_warehouse_id_qty_tier_by_mov), 2).alias("unit_price_plus_qty_avg_2_prior"),
        F.rank().over(w_sku_id_by_moved_at).alias("moved_at_rank"),
        F.round(F.avg("unit_price").over(w_warehouse_id_qty_tier_by_mov_2), 2).alias("unit_price_avg_1_prior"),
        F.round(F.avg("unit_price").over(w_qty_tier_move_type), 2).alias("qty_tier_move_type_avg_unit_price"),
        (F.col("unit_price") - F.lag("unit_price").over(w_sku_id_by_moved_at)).alias("unit_price_change"),
        F.round(F.percent_rank().over(w_warehouse_id_qty_tier_by_mov_3), 3).alias("moved_at_percent_rank"),
        F.first("unit_price").over(w_qty_tier_sku_id_by_moved_at_2).alias("first_unit_price"),
    )
)

top_per_group = metrics.filter(F.col("moved_at_rank") <= 3)

result = (
    top_per_group
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "qty_tier",
        "unit_price_plus_qty",
        "unit_price_plus_qty_vs_move_type_avg",
        "distinct_operator_in_qty_tier_move_type",
        "unit_price_plus_qty_vs_qty_tier_move_type_avg",
        "next_unit_price",
        "unit_price_plus_qty_avg_2_prior",
        "moved_at_rank",
        "qty_tier_move_type_avg_unit_price",
        "unit_price_change",
        "moved_at_percent_rank",
        "first_unit_price",
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
    )
    .orderBy(F.col("qty_tier").desc(), "id")
    .limit(5)
)
