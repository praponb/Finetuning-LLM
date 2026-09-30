from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_moved_at_pri = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(-1, -1)
w_sku_category = Window.partitionBy("sku_category")
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_warehouse_id_by_moved_at_ful = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_all_by_moved_at_running = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator = Window.partitionBy("operator")
w_sku_category_sku_id_by_moved = Window.partitionBy("sku_category", "sku_id").orderBy(F.col("moved_at").desc())
w_sku_category_sku_id_by_moved_2 = Window.partitionBy("sku_category", "sku_id").orderBy(F.col("moved_at").desc(), "id")
w_move_type_by_moved_at_ties = Window.partitionBy("move_type").orderBy(F.col("moved_at").asc_nulls_last())

valid = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date")) & (F.col("warehouse_id") >= 2))
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

with_details = (
    valid.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

with_trends = (
    with_details
    .select(
        "*",
        F.round(F.avg("unit_price").over(w_warehouse_id_by_moved_at_pri), 2).alias("unit_price_avg_1_prior"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_category), 2).alias("unit_price_vs_sku_category_avg"),
        F.ntile(5).over(w_warehouse_id_by_moved_at).alias("moved_at_quintile"),
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.nth_value("moved_at", 2).over(w_warehouse_id_by_moved_at_ful).alias("second_moved_at"),
        F.lead("moved_at").over(w_all_by_moved_at).alias("next_moved_at"),
        F.lag("unit_price", 1, 0).over(w_all_by_moved_at).alias("prev_unit_price"),
        F.count(F.lit(1)).over(w_all_by_moved_at_running).alias("running_count"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator), F.lit(0)), 4).alias("unit_price_share_of_operator"),
        F.round(F.cume_dist().over(w_sku_category_sku_id_by_moved), 3).alias("moved_at_cume_dist"),
        F.max("unit_price").over(w_sku_category).alias("sku_category_max_unit_price"),
        F.round(F.avg("unit_price").over(w_sku_category), 2).alias("sku_category_avg_unit_price"),
        F.lag("moved_at").over(w_sku_category_sku_id_by_moved_2).alias("prev_moved_at"),
        F.rank().over(w_move_type_by_moved_at_ties).alias("moved_at_rank"),
    )
)

rollup_base = (
    with_trends
    .groupBy("sku_id")
    .agg(
        F.sum(F.when(F.col("move_type").isNotNull(), F.col("running_count")).otherwise(0)).alias("running_count_move_type_is_not_null"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.round(F.avg("unit_price_avg_1_prior"), 2).alias("avg_unit_price_avg_1_prior"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    rollup_base
    .select(
        "sku_id",
        "distinct_operators",
        "avg_unit_price_avg_1_prior",
        "row_count",
    )
    .orderBy(F.col("avg_unit_price_avg_1_prior").asc(), "distinct_operators", "sku_id")
    .limit(5)
)
