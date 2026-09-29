from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_unit_price_per = Window.partitionBy("move_type").orderBy(F.col("unit_price_per_qty_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type = Window.partitionBy("move_type")
w_move_type_by_unit_price_per_2 = Window.partitionBy("move_type").orderBy(F.col("unit_price_per_qty_bucket").desc(), "id")
w_move_type_by_unit_price_per_3 = Window.partitionBy("move_type").orderBy(F.col("unit_price_per_qty_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
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

enriched = (
    filtered_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "move_type",
        "unit_price",
        "qty_bucket",
        F.round(F.col("unit_price") / F.nullif(F.col("qty_bucket"), F.lit(0)), 3).alias("unit_price_per_qty_bucket"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.first("unit_price_per_qty_bucket", ignorenulls=True).over(w_move_type_by_unit_price_per).alias("first_known_unit_price_per_qty_bucket"),
        F.round(F.col("qty_bucket") - F.avg("qty_bucket").over(w_move_type), 2).alias("qty_bucket_vs_move_type_avg"),
        F.lag("unit_price_per_qty_bucket", 2, 0).over(w_move_type_by_unit_price_per_2).alias("prev2_unit_price_per_qty_bucket"),
        F.last("unit_price").over(w_move_type_by_unit_price_per_3).alias("last_unit_price"),
    )
)

move_type_stats = (
    with_trends
    .groupBy("move_type")
    .agg(F.percentile_approx("first_known_unit_price_per_qty_bucket", 0.5).alias("median_first_known_unit_price_per_qty_bucket"))
)

vs_group = (
    with_trends.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.first_known_unit_price_per_qty_bucket") >= F.col("g.median_first_known_unit_price_per_qty_bucket"))
    .select(
        "s.*",
        "g.median_first_known_unit_price_per_qty_bucket",
        F.round(F.col("s.first_known_unit_price_per_qty_bucket") - F.col("g.median_first_known_unit_price_per_qty_bucket"), 2).alias("first_known_unit_price_per_qty_bucket_minus_medi"),
    )
)

with_activity = (
    vs_group
    .crossJoin(vs_group.agg(F.min("unit_price_per_qty_bucket").alias("overall_min_unit_price_per_qty_bucket")))
    .filter(F.col("unit_price_per_qty_bucket") > F.col("overall_min_unit_price_per_qty_bucket"))
    .drop("overall_min_unit_price_per_qty_bucket")
)

grouped = (
    with_activity
    .groupBy("move_type")
    .agg(
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.round(F.avg("last_unit_price"), 2).alias("avg_last_unit_price"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("unit_price").alias("sum_unit_price_check"),
    )
    .filter(F.col("sum_unit_price_check") > 111.7)
    .drop("sum_unit_price_check")
)

result = (
    grouped
    .filter(F.col("avg_last_unit_price") >= 100.8)
    .select(
        "move_type",
        "move_type_list",
        "avg_last_unit_price",
        "distinct_ids",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.coalesce(F.col("avg_last_unit_price"), F.lit(0.0)).alias("avg_last_unit_price_filled"),
    )
    .orderBy(F.col("move_type_len").desc(), "move_type")
    .limit(20)
)
