from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_all_by_days_since_moved_at_t = Window.orderBy(F.col("days_since_moved_at").desc())
w_all_by_days_since_moved_at_f = Window.orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_moved_at = Window.orderBy("days_since_moved_at", "id")
w_all_by_days_since_moved_at_r = Window.orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_all_by_days_since_moved_at_2 = Window.orderBy(F.col("days_since_moved_at").desc(), "id")
w_sku_id_warehouse_id_by_days = Window.partitionBy("sku_id", "warehouse_id").orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_stock_moves
    .filter((F.col("qty").between(137, 300)) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "days_since_moved_at",
        "moved_at_label",
        F.sum("unit_price").over(w_sku_id).alias("sku_id_sum_unit_price"),
        F.dense_rank().over(w_all_by_days_since_moved_at_t).alias("days_since_moved_at_dense_rank"),
        F.nth_value("unit_price", 2).over(w_all_by_days_since_moved_at_f).alias("second_unit_price"),
        F.lag("days_since_moved_at").over(w_all_by_days_since_moved_at).alias("prev_days_since_moved_at"),
        F.min("days_since_moved_at").over(w_sku_id).alias("sku_id_min_days_since_moved_at"),
        F.count(F.lit(1)).over(w_all_by_days_since_moved_at_r).alias("running_count"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id), F.lit(0)), 4).alias("unit_price_share_of_sku_id"),
        F.max("unit_price").over(w_sku_id).alias("sku_id_max_unit_price"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id), 2).alias("unit_price_vs_sku_id_avg"),
        F.sum("days_since_moved_at").over(w_sku_id).alias("sku_id_sum_days_since_moved_at"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type_operator), F.lit(0)), 4).alias("unit_price_share_of_move_type_operator"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.row_number().over(w_all_by_days_since_moved_at_2).alias("days_since_moved_at_seq"),
        F.count(F.lit(1)).over(w_move_type_operator).alias("rows_in_move_type_operator"),
        F.last("unit_price").over(w_sku_id_warehouse_id_by_days).alias("last_unit_price"),
    )
)

result = (
    metrics
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "sku_id_sum_unit_price",
        "days_since_moved_at_dense_rank",
        "second_unit_price",
        "prev_days_since_moved_at",
        "sku_id_min_days_since_moved_at",
        "running_count",
        "unit_price_share_of_sku_id",
        "sku_id_max_unit_price",
        "sku_id_sum_days_since_moved_at",
        "rows_in_sku_id",
        "days_since_moved_at_seq",
        "rows_in_move_type_operator",
        "last_unit_price",
        F.greatest(F.col("last_unit_price"), F.col("sku_id_max_unit_price")).alias("greatest_last_unit_price_sku_id_max_unit_price"),
    )
    .orderBy(F.col("last_unit_price").asc(), "id")
    .limit(10)
)
