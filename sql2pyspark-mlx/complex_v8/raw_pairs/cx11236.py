from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_moved_at_f = Window.orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_sku_is_active_by_d = Window.partitionBy("move_type", "sku_is_active").orderBy(F.col("days_since_moved_at").desc(), "id")
w_sku_id_by_days_since_moved_a = Window.partitionBy("sku_id").orderBy("days_since_moved_at", "id").rowsBetween(-3, -1)
w_move_type_by_prev_days_since = Window.partitionBy("move_type").orderBy(F.col("prev_days_since_moved_at").desc_nulls_first(), "id")
w_move_type_by_prev_days_since_2 = Window.partitionBy("move_type").orderBy("prev_days_since_moved_at", "id")

cleaned_moves = (
    df_stock_moves
    .filter(F.col("operator") == "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.year(F.col("moved_at")).alias("moved_at_year"),
    )
)

with_parent = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.size(F.col("sk.labels")) >= 1), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.days_since_moved_at",
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.first("days_since_moved_at").over(w_all_by_days_since_moved_at_f).alias("first_days_since_moved_at"),
        F.lag("days_since_moved_at").over(w_move_type_sku_is_active_by_d).alias("prev_days_since_moved_at"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "operator",
        "days_since_moved_at",
        F.round(F.avg("prev_days_since_moved_at").over(w_sku_id_by_days_since_moved_a), 2).alias("prev_days_since_moved_at_avg_3_prior"),
        F.lag("days_since_moved_at", 1, 0).over(w_move_type_by_prev_days_since).alias("prev_days_since_moved_at_2"),
        F.dense_rank().over(w_move_type_by_prev_days_since_2).alias("prev_days_since_moved_at_dense_rank"),
    )
)

per_group = (
    with_windows
    .groupBy("operator", "prev_days_since_moved_at_dense_rank")
    .agg(
        F.min("prev_days_since_moved_at_dense_rank").alias("min_prev_days_since_moved_at_dense_rank"),
        F.max("days_since_moved_at").alias("max_days_since_moved_at"),
        F.min("prev_days_since_moved_at_2").alias("min_prev_days_since_moved_at_2"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 2)
)

labeled = (
    per_group
    .select(
        "*",
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.when(F.col("operator").isin("op_chan", "op_dew"), "group_a").when(F.col("operator") == "op_eve", "group_b").otherwise("other").alias("operator_group"),
    )
)

result = (
    labeled
    .select(
        "operator",
        "prev_days_since_moved_at_dense_rank",
        "row_count",
        "max_days_since_moved_at",
    )
    .unpivot(
        ids=["operator", "prev_days_since_moved_at_dense_rank"],
        values=["row_count", "max_days_since_moved_at"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
