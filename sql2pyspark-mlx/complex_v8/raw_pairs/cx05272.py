from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_labels_count_by_la = Window.partitionBy("is_active", "labels_count").orderBy(F.col("labels_count").desc())
w_is_active = Window.partitionBy("is_active")
w_labels_count_category_by_lab = Window.partitionBy("labels_count", "category").orderBy("labels_count", "id").rowsBetween(-2, Window.currentRow)

valid_skus = (
    df_skus
    .filter(F.col("weight_kg") <= 14.3)
    .select(
        "id",
        "category",
        "labels",
        "is_active",
        F.size(F.col("labels")).alias("labels_count"),
        F.lower(F.trim(F.col("category"))).alias("category_clean"),
    )
)

with_trends = (
    valid_skus
    .select(
        "*",
        F.rank().over(w_is_active_labels_count_by_la).alias("labels_count_rank"),
        F.size(F.collect_set("category").over(w_is_active)).alias("distinct_category_in_is_active"),
        F.sum("labels_count").over(w_labels_count_category_by_lab).alias("labels_count_sum_3"),
        F.round(F.col("labels_count") / F.nullif(F.sum("labels_count").over(w_is_active), F.lit(0)), 4).alias("labels_count_share_of_is_active"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"), F.count("*").alias("move_count_2"))
)

with_counts = (
    with_trends.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

qualified = (
    with_counts
    .crossJoin(with_counts.agg(F.max("labels_count").alias("overall_max_labels_count")))
    .filter(F.col("labels_count") < F.col("overall_max_labels_count"))
    .drop("overall_max_labels_count")
)

rollup_base = (
    qualified
    .groupBy("labels_count", "distinct_category_in_is_active")
    .agg(
        F.percentile_approx("labels_count", 0.5).alias("median_labels_count"),
        F.array_sort(F.collect_set("category")).alias("category_list"),
        F.max("move_count_2").alias("max_move_count_2"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

without_matches = (
    rollup_base
    .crossJoin(rollup_base.agg(F.min("labels_count").alias("overall_min_labels_count")))
    .filter(F.col("labels_count") > F.col("overall_min_labels_count"))
    .drop("overall_min_labels_count")
)

scored = (
    without_matches
    .select(
        "labels_count",
        "distinct_category_in_is_active",
        "max_move_count_2",
        "n_rows",
        F.least(F.col("max_move_count_2"), F.col("median_labels_count")).alias("least_max_move_count_2_median_labels_count"),
    )
)

result = (
    scored
    .select(
        "labels_count",
        "distinct_category_in_is_active",
        "n_rows",
        "least_max_move_count_2_median_labels_count",
        F.round(F.col("n_rows") / F.nullif(F.col("labels_count"), F.lit(0)), 3).alias("n_rows_per_labels_count"),
        F.abs(F.col("n_rows") - 8).alias("n_rows_distance"),
    )
    .orderBy(F.col("distinct_category_in_is_active").desc())
)
