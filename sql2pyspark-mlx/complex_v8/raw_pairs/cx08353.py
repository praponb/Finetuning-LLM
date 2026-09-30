from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_move_type_by_moved_at = Window.partitionBy("move_type").orderBy("moved_at", "id")
w_move_type_by_moved_at_prior = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(-6, -1)
w_sku_id_by_unit_price_moving = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w_move_type_by_unit_price = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), "id")
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_next_moved_at_month_by_days = Window.partitionBy(F.date_trunc("month", "next_moved_at")).orderBy("days_since_moved_at_sum_6_prior", "id")
w_move_type = Window.partitionBy("move_type")
w_next_moved_at_month_by_days_2 = Window.partitionBy(F.date_trunc("month", "next_moved_at")).orderBy("days_since_moved_at_sum_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_move_type_in_sku_id = Window.partitionBy("distinct_move_type_in_sku_id", "move_type").orderBy("distinct_move_type_in_sku_id", "id")

cleaned_moves = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

metrics = (
    cleaned_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.max("days_since_moved_at").over(w_sku_id).alias("sku_id_max_days_since_moved_at"),
        F.round(F.avg("qty").over(w_sku_id), 2).alias("sku_id_avg_qty"),
        F.lag("qty").over(w_move_type_by_moved_at).alias("prev_qty"),
        F.sum("days_since_moved_at").over(w_move_type_by_moved_at_prior).alias("days_since_moved_at_sum_6_prior"),
        F.round(F.avg("days_since_moved_at").over(w_sku_id), 2).alias("sku_id_avg_days_since_moved_at"),
        F.sum("days_since_moved_at").over(w_sku_id).alias("sku_id_sum_days_since_moved_at"),
        F.round(F.avg("days_since_moved_at").over(w_sku_id_by_unit_price_moving), 2).alias("days_since_moved_at_avg_7"),
        F.size(F.collect_set("move_type").over(w_sku_id)).alias("distinct_move_type_in_sku_id"),
        F.lag("unit_price").over(w_move_type_by_unit_price).alias("prev_unit_price"),
        F.max("qty").over(w_sku_id).alias("sku_id_max_qty"),
        F.min("unit_price").over(w_sku_id).alias("sku_id_min_unit_price"),
        F.lead("moved_at").over(w_sku_id_by_unit_price).alias("next_moved_at"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.ntile(4).over(w_next_moved_at_month_by_days).alias("days_since_moved_at_sum_6_prior_quartile"),
        F.round(F.avg("days_since_moved_at_avg_7").over(w_move_type), 2).alias("move_type_avg_days_since_moved_at_avg_7"),
        F.last("prev_qty").over(w_next_moved_at_month_by_days_2).alias("last_prev_qty"),
        F.ntile(5).over(w_distinct_move_type_in_sku_id).alias("distinct_move_type_in_sku_id_quintile"),
    )
)

result = (
    ranked
    .filter(F.col("days_since_moved_at_sum_6_prior_quartile") > 4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "sku_id_max_days_since_moved_at",
        "sku_id_avg_qty",
        "prev_qty",
        "days_since_moved_at_sum_6_prior",
        "sku_id_avg_days_since_moved_at",
        "sku_id_sum_days_since_moved_at",
        "sku_id_max_qty",
        "sku_id_min_unit_price",
        "next_moved_at",
        "days_since_moved_at_sum_6_prior_quartile",
        "move_type_avg_days_since_moved_at_avg_7",
        "last_prev_qty",
        "distinct_move_type_in_sku_id_quintile",
        F.date_trunc("quarter", F.col("next_moved_at")).alias("next_moved_at_quarter"),
    )
    .orderBy(F.col("sku_id_avg_days_since_moved_at").desc())
)
