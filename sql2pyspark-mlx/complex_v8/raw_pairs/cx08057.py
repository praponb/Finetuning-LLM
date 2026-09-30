from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_category_weight_kg_tier = Window.partitionBy("category", "weight_kg_tier")
w_all_by_move_count_ties = Window.orderBy("move_count")
w_all_by_move_count_full = Window.orderBy("move_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_move_count_running = Window.orderBy("move_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_warehouse_id = Window.partitionBy("move_warehouse_id")
w_move_count_is_active_by_move = Window.partitionBy("move_count", "is_active").orderBy("move_count", "id", "move_id").rowsBetween(-6, Window.currentRow)
w_move_count_is_active_by_move_2 = Window.partitionBy("move_count", "is_active").orderBy("move_count", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_move_count_rank = Window.orderBy("move_count_rank", "id", "move_id")
w_move_count_is_active_by_move_3 = Window.partitionBy("move_count", "is_active").orderBy("move_count", "id", "move_id")
w_move_count_is_active_by_move_4 = Window.partitionBy("move_count", "is_active").orderBy("move_count", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_skus = (
    df_skus
    .filter(F.col("is_active"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "is_active",
        F.when(F.col("weight_kg") >= 20.8, "large").when(F.col("weight_kg") >= 6.4, "medium").otherwise("small").alias("weight_kg_tier"),
        F.length(F.trim(F.col("category"))).alias("category_len"),
    )
)

with_activity = (
    valid_skus
    .crossJoin(valid_skus.agg(F.min("category_len").alias("overall_min_category_len")))
    .filter(F.col("category_len") > F.col("overall_min_category_len"))
    .drop("overall_min_category_len")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.sum("unit_price").alias("total_move_unit_price"),
    )
)

profiled = (
    with_activity.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.total_move_unit_price",
    )
)

metrics = (
    profiled
    .select(
        "*",
        F.rank().over(w_all_by_move_count_ties).alias("move_count_rank"),
        F.round(F.col("total_move_unit_price") / F.nullif(F.sum("total_move_unit_price").over(w_category_weight_kg_tier), F.lit(0)), 4).alias("total_move_unit_price_share_of_category_weight_k"),
        F.first("category_len").over(w_all_by_move_count_full).alias("first_category_len"),
        F.sum("move_count").over(w_all_by_move_count_running).alias("running_move_count"),
    )
)

detail_rows = (
    metrics.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.sku_code",
        "s.category",
        "s.unit_cost",
        "s.is_active",
        "s.weight_kg_tier",
        "s.move_count",
        "s.total_move_unit_price",
        "s.move_count_rank",
        "s.first_category_len",
        "s.running_move_count",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.qty").alias("move_qty"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.max("moved_at").alias("last_moved_at_2"),
    )
)

with_counts = (
    detail_rows.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select("s.*", "st.last_moved_at", "st.last_moved_at_2")
)

with_windows = (
    with_counts
    .select(
        "id",
        "sku_code",
        "category",
        "is_active",
        "total_move_unit_price",
        "move_count_rank",
        "first_category_len",
        "running_move_count",
        "move_id",
        "move_qty",
        "last_moved_at",
        F.round(F.col("first_category_len") - F.avg("first_category_len").over(w_move_warehouse_id), 2).alias("first_category_len_vs_move_warehouse_id_avg"),
        F.round(F.avg("running_move_count").over(w_move_count_is_active_by_move), 2).alias("running_move_count_avg_7"),
        F.nth_value("move_qty", 2).over(w_move_count_is_active_by_move_2).alias("second_move_qty"),
        F.row_number().over(w_all_by_move_count_rank).alias("move_count_rank_seq"),
        F.row_number().over(w_move_count_is_active_by_move_3).alias("move_count_seq"),
        F.min("move_count_rank").over(w_move_count_is_active_by_move_4).alias("min_move_count_rank_so_far"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["move_count_rank", "move_id", "move_count_rank_seq"], ["move_count_rank"], ["move_id"], []],
        "move_count_rank", "move_id", "move_count_rank_seq",
    )
    .agg(F.grouping_id().alias("gid"), F.count("*").alias("cnt"))
    .orderBy(
        F.col("move_count_rank").asc_nulls_last(),
        F.col("move_id").asc_nulls_last(),
        F.col("move_count_rank_seq").asc_nulls_last(),
    )
)
