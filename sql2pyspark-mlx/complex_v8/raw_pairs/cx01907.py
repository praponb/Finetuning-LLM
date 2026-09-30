from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_all_by_unit_price_vs_sku_id = Window.orderBy("unit_price_vs_sku_id_avg", "id")
w_all_by_unit_price_vs_sku_id_2 = Window.orderBy("unit_price_vs_sku_id_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price_vs_sku_id_3 = Window.orderBy("unit_price_vs_sku_id_avg", "id").rowsBetween(-3, Window.currentRow)
w_operator_move_type = Window.partitionBy("operator", "move_type")
w_move_type_qty_max_4_tier = Window.partitionBy("move_type", "qty_max_4_tier")
w_move_type_by_max_qty_prior_2 = Window.partitionBy("move_type").orderBy(F.col("max_qty").desc(), "move_type", "qty_max_4_tier").rowsBetween(-2, -1)
w_move_type_by_max_qty_full = Window.partitionBy("move_type").orderBy(F.col("max_qty").desc(), "move_type", "qty_max_4_tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

ranked = (
    filtered_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id), 2).alias("unit_price_vs_sku_id_avg"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
    )
)

sequenced = (
    ranked
    .select(
        "*",
        F.lag("qty", 3).over(w_all_by_unit_price_vs_sku_id).alias("prev3_qty"),
        F.lag("qty").over(w_all_by_unit_price_vs_sku_id).alias("prev_qty"),
        F.count(F.lit(1)).over(w_all_by_unit_price_vs_sku_id_2).alias("running_count"),
        F.max("qty").over(w_all_by_unit_price_vs_sku_id_3).alias("qty_max_4"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator_move_type), F.lit(0)), 4).alias("unit_price_share_of_operator_move_type"),
        F.sum("unit_price_vs_sku_id_avg").over(w_operator_move_type).alias("operator_move_type_sum_unit_price_vs_sku_id_avg"),
    )
)

rollup_base = (
    sequenced
    .groupBy(
        "move_type",
        F.when(F.col("qty_max_4") >= 1050, "HIGH").when(F.col("qty_max_4") >= 376, "MEDIUM").otherwise("LOW").alias("qty_max_4_tier"),
    )
    .agg(
        F.min("qty_max_4").alias("min_qty_max_4"),
        F.max("qty").alias("max_qty"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.avg("running_count").alias("avg_running_count_check"),
    )
    .filter(F.col("avg_running_count_check") > 9)
    .drop("avg_running_count_check")
)

sequenced_2 = (
    rollup_base
    .select(
        "*",
        F.sum("max_qty").over(w_move_type_qty_max_4_tier).alias("move_type_qty_max_4_tier_sum_max_qty"),
        F.max("min_qty_max_4").over(w_move_type_by_max_qty_prior_2).alias("min_qty_max_4_max_2_prior"),
        F.first("max_qty").over(w_move_type_by_max_qty_full).alias("first_max_qty"),
    )
)

filtered = (
    sequenced_2
    .crossJoin(sequenced_2.agg(F.min("max_qty").alias("overall_min_max_qty")))
    .filter(F.col("max_qty") > F.col("overall_min_max_qty"))
    .drop("overall_min_max_qty")
)

result = (
    filtered
    .filter(F.col("first_max_qty") < 400)
    .select(
        "move_type",
        "qty_max_4_tier",
        "min_qty_max_4",
        "move_type_list",
        "move_type_qty_max_4_tier_sum_max_qty",
        "min_qty_max_4_max_2_prior",
        "first_max_qty",
        F.concat(F.col("qty_max_4_tier"), F.lit("#"), F.col("first_max_qty").cast("string")).alias("qty_max_4_tier_ref"),
        F.abs(F.col("min_qty_max_4") - 900).alias("min_qty_max_4_distance"),
    )
    .orderBy(
        F.col("qty_max_4_tier_ref").asc_nulls_last(),
        F.col("move_type_qty_max_4_tier_sum_max_qty").desc(),
    )
)
