from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_move_type_by = Window.partitionBy(F.month("moved_at"), "move_type").orderBy("moved_at", "id")
w_sku_category_sku_is_active_b = Window.partitionBy("sku_category", "sku_is_active").orderBy(F.col("qty").asc_nulls_last(), "id")
w_sku_is_active_warehouse_city = Window.partitionBy("sku_is_active", "warehouse_city")
w_sku_id = Window.partitionBy("sku_id")
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_moved_at_month_move_type_by_2 = Window.partitionBy(F.month("moved_at"), "move_type").orderBy("moved_at")
w_all_by_moved_at_running = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_city_warehouse_id = Window.partitionBy("warehouse_city", "warehouse_id").orderBy("distinct_warehouse_id_in_sku_is_active_warehouse", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_city_warehouse_id_2 = Window.partitionBy("warehouse_city", "warehouse_id").orderBy("distinct_warehouse_id_in_sku_is_active_warehouse")
w_moved_at_rank_qty_seq = Window.partitionBy("moved_at_rank", "qty_seq")
w_distinct_warehouse_id_in_sku = Window.partitionBy("distinct_warehouse_id_in_sku_id", "sku_id").orderBy(F.col("running_qty").desc(), "id")

recent = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("moved_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("qty") >= 249, "HIGH").when(F.col("qty") >= 152, "MEDIUM").otherwise("LOW").alias("qty_tier"),
    )
)

joined_sku = (
    recent.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("sk.is_active"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.qty_tier",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.city").alias("warehouse_city"),
    )
)

with_trends = (
    joined_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "sku_category",
        "sku_labels",
        "warehouse_city",
        F.round(F.cume_dist().over(w_moved_at_month_move_type_by), 3).alias("moved_at_cume_dist"),
        F.row_number().over(w_sku_category_sku_is_active_b).alias("qty_seq"),
        F.round(F.avg("qty").over(w_sku_is_active_warehouse_city), 2).alias("sku_is_active_warehouse_city_avg_qty"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
        F.round(F.cume_dist().over(w_all_by_moved_at), 3).alias("moved_at_cume_dist_2"),
        F.max("qty").over(w_sku_id).alias("sku_id_max_qty"),
        F.rank().over(w_moved_at_month_move_type_by_2).alias("moved_at_rank"),
        F.size(F.collect_set("warehouse_id").over(w_sku_is_active_warehouse_city)).alias("distinct_warehouse_id_in_sku_is_active_warehouse"),
        F.sum("qty").over(w_all_by_moved_at_running).alias("running_qty"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "sku_labels",
        "moved_at_cume_dist",
        "qty_seq",
        "distinct_warehouse_id_in_sku_id",
        "moved_at_rank",
        "distinct_warehouse_id_in_sku_is_active_warehouse",
        "running_qty",
        F.count(F.lit(1)).over(w_warehouse_city_warehouse_id).alias("running_count"),
        F.round(F.cume_dist().over(w_warehouse_city_warehouse_id_2), 3).alias("distinct_warehouse_id_in_sku_is_active_warehouse_2"),
        F.size(F.collect_set("warehouse_city").over(w_moved_at_rank_qty_seq)).alias("distinct_warehouse_city_in_moved_at_rank_qty_seq"),
        F.min("running_qty").over(w_moved_at_rank_qty_seq).alias("moved_at_rank_qty_seq_min_running_qty"),
        (F.col("qty") - F.lag("qty").over(w_distinct_warehouse_id_in_sku)).alias("qty_change"),
        F.size(F.collect_set("sku_id").over(w_moved_at_rank_qty_seq)).alias("distinct_sku_id_in_moved_at_rank_qty_seq"),
    )
)

rollup_base = (
    sequenced
    .filter(F.col("distinct_warehouse_id_in_sku_id") >= 4)
    .groupBy("distinct_warehouse_id_in_sku_id")
    .agg(
        F.sum("distinct_sku_id_in_moved_at_rank_qty_seq").alias("total_distinct_sku_id_in_moved_at_rank_qty_seq"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.round(F.avg("running_qty"), 2).alias("avg_running_qty"),
        F.count("*").alias("row_count"),
        F.min("moved_at_rank").alias("min_moved_at_rank"),
        F.round(F.avg("distinct_warehouse_id_in_sku_id"), 2).alias("avg_distinct_warehouse_id_in_sku_id"),
    )
)

result = (
    rollup_base
    .filter(F.col("avg_distinct_warehouse_id_in_sku_id").between(2.8, 9.1))
    .select(
        "distinct_warehouse_id_in_sku_id",
        "avg_running_qty",
        "avg_distinct_warehouse_id_in_sku_id",
    )
    .orderBy("avg_running_qty", "avg_distinct_warehouse_id_in_sku_id")
)
