from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_is_active_by_unit_c = Window.partitionBy("category", "is_active").orderBy(F.col("unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_category_category_or_unknown = Window.partitionBy("category", "category_or_unknown")
w_is_active_by_unit_cost_minus = Window.partitionBy("is_active").orderBy("unit_cost_minus_weight_kg", "id")

valid = (
    df_skus
    .filter(F.col("weight_kg").between(1.8, 21.1))
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        (F.col("unit_cost") - F.col("weight_kg")).alias("unit_cost_minus_weight_kg"),
        F.coalesce(F.col("category"), F.lit("UNKNOWN")).alias("category_or_unknown"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "category",
        "unit_cost",
        "weight_kg",
        "is_active",
        "category_or_unknown",
        F.min("weight_kg").over(w_category_is_active_by_unit_c).alias("min_weight_kg_so_far"),
        F.first("unit_cost", ignorenulls=True).over(w_category_is_active_by_unit_c).alias("first_known_unit_cost"),
        F.round(F.col("unit_cost") / F.nullif(F.sum("unit_cost").over(w_category_category_or_unknown), F.lit(0)), 4).alias("unit_cost_share_of_category_category_or_unknown"),
        F.sum("unit_cost").over(w_category_category_or_unknown).alias("category_category_or_unknown_sum_unit_cost"),
        F.max("weight_kg").over(w_category_category_or_unknown).alias("category_category_or_unknown_max_weight_kg"),
        F.lag("unit_cost").over(w_is_active_by_unit_cost_minus).alias("prev_unit_cost"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

with_stats = (
    metrics.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
        "st.first_moved_at",
    )
)

result = (
    with_stats
    .groupingSets(
        [["move_count_2", "category_or_unknown"], ["move_count_2"], []],
        "move_count_2", "category_or_unknown",
    )
    .agg(
        F.grouping("move_count_2").alias("is_total_move_count_2"),
        F.round(F.sum("unit_cost"), 2).alias("total_unit_cost"),
        F.round(100 * F.count(F.when(~F.col("is_active"), 1)) / F.count("*"), 1).alias("pct_not_is_active"),
    )
)
