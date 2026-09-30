from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_ties = Window.orderBy(F.col("qty").asc_nulls_last())
w_sku_id_by_qty_prior_6 = Window.partitionBy("sku_id").orderBy(F.col("qty").desc(), "id").rowsBetween(-6, -1)
w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_qty_running = Window.partitionBy("sku_id").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty = Window.orderBy(F.col("qty").asc_nulls_last(), "id")
w_warehouse_id_sku_id_by_moved_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_sku_category = Window.partitionBy("sku_id", "sku_category")
w_sku_id_by_qty = Window.partitionBy("sku_id").orderBy(F.col("qty").desc(), "id")
w_distinct_warehouse_id_in_sku = Window.partitionBy("distinct_warehouse_id_in_sku_id_sku_category")
w_all_by_qty_rank = Window.orderBy("qty_rank", "id")

recent_moves = (
    df_stock_moves
    .filter(~F.col("move_type").isin("OUTBOUND", "ADJUST"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

enriched_sku = (
    recent_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "sk.sku_code",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

with_trends = (
    enriched_sku
    .select(
        "id",
        "qty",
        "moved_at",
        "unit_price",
        "sku_labels",
        "sku_category",
        F.rank().over(w_all_by_qty_ties).alias("qty_rank"),
        F.sum("unit_price").over(w_sku_id_by_qty_prior_6).alias("unit_price_sum_6_prior"),
        F.sum("unit_price").over(w_warehouse_id_sku_id_by_moved).alias("running_unit_price"),
        F.count(F.lit(1)).over(w_sku_id_by_qty_running).alias("running_count"),
        F.ntile(4).over(w_all_by_qty).alias("qty_quartile"),
        F.first("qty").over(w_warehouse_id_sku_id_by_moved_2).alias("first_qty"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_sku_category), 2).alias("unit_price_vs_sku_id_sku_category_avg"),
        F.max("unit_price").over(w_sku_id_sku_category).alias("sku_id_sku_category_max_unit_price"),
        F.sum("qty").over(w_sku_id_sku_category).alias("sku_id_sku_category_sum_qty"),
        F.rank().over(w_all_by_qty).alias("qty_rank_2"),
        F.row_number().over(w_all_by_qty).alias("seq_no"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id_sku_category)).alias("distinct_warehouse_id_in_sku_id_sku_category"),
        F.round(F.percent_rank().over(w_all_by_qty_ties), 3).alias("qty_percent_rank"),
        F.round(F.cume_dist().over(w_sku_id_by_qty), 3).alias("qty_cume_dist"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.max("running_unit_price").over(w_distinct_warehouse_id_in_sku).alias("distinct_warehouse_id_in_sku_id_sku_category_max"),
        F.size(F.collect_set("sku_category").over(w_distinct_warehouse_id_in_sku)).alias("distinct_sku_category_in_distinct_warehouse_id_i"),
        F.min("qty_rank").over(w_distinct_warehouse_id_in_sku).alias("distinct_warehouse_id_in_sku_id_sku_category_min"),
        F.round(F.cume_dist().over(w_all_by_qty_rank), 3).alias("qty_rank_cume_dist"),
        F.count(F.lit(1)).over(w_distinct_warehouse_id_in_sku).alias("rows_in_distinct_warehouse_id_in_sku_id_sku_cate"),
    )
)

result = (
    metrics
    .filter(F.col("rows_in_distinct_warehouse_id_in_sku_id_sku_cate") >= 7)
    .select(
        "id",
        "unit_price",
        "sku_category",
        "qty_rank",
        "unit_price_sum_6_prior",
        "running_unit_price",
        "running_count",
        "qty_quartile",
        "unit_price_vs_sku_id_sku_category_avg",
        "sku_id_sku_category_max_unit_price",
        "sku_id_sku_category_sum_qty",
        "qty_rank_2",
        "seq_no",
        "distinct_warehouse_id_in_sku_id_sku_category",
        "qty_percent_rank",
        "distinct_sku_category_in_distinct_warehouse_id_i",
        "distinct_warehouse_id_in_sku_id_sku_category_min",
        "qty_rank_cume_dist",
    )
    .orderBy(F.col("sku_category").asc_nulls_last())
)
