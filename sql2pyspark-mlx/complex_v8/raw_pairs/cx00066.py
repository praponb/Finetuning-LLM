from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_moved_at_ties = Window.partitionBy("sku_id").orderBy(F.col("moved_at").asc_nulls_last())
w_sku_id_by_moved_at_full = Window.partitionBy("sku_id").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at = Window.orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_all_by_moved_at_ties = Window.orderBy(F.col("moved_at").asc_nulls_last())

valid_moves = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp")) & (F.col("sku_id") <= 8))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_windows = (
    valid_moves
    .select(
        "*",
        F.dense_rank().over(w_sku_id_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.nth_value("unit_price", 2).over(w_sku_id_by_moved_at_full).alias("second_unit_price"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.lag("unit_price").over(w_all_by_moved_at).alias("prev_unit_price"),
        F.ntile(10).over(w_all_by_moved_at).alias("moved_at_decile"),
        F.rank().over(w_all_by_moved_at_ties).alias("moved_at_rank"),
    )
)

enriched_sku = (
    windowed.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.second_unit_price",
        "s.moved_at_decile",
        "s.moved_at_rank",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
        F.col("sk.labels").alias("sku_labels"),
    )
)

operator_stats = (
    enriched_sku
    .groupBy("operator")
    .agg(F.percentile_approx("moved_at_decile", 0.5).alias("median_moved_at_decile"))
)

outliers = (
    enriched_sku.alias("s")
    .join(operator_stats.alias("g"), F.col("g.operator") == F.col("s.operator"), "inner")
    .filter(F.col("s.moved_at_decile") >= F.col("g.median_moved_at_decile"))
    .select(
        "s.*",
        "g.median_moved_at_decile",
        F.round(F.col("s.moved_at_decile") - F.col("g.median_moved_at_decile"), 2).alias("moved_at_decile_minus_median"),
    )
)

result = (
    outliers
    .filter(F.col("moved_at_decile") <= 8)
    .select(
        "id",
        "sku_id",
        "operator",
        "unit_price",
        "sku_weight_kg",
        "sku_code",
        F.array_contains(F.col("sku_labels"), "fragile").alias("has_fragile"),
        F.array_join(F.array_sort(F.col("sku_labels")), ",").alias("sku_labels_text"),
    )
    .orderBy("sku_id", "id")
)
