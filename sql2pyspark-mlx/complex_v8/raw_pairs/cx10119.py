from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_count = Window.partitionBy("move_count")
w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())
w_all_by_qty_prior_2 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-2, -1)
w_all_by_qty_prior_1 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-1, -1)
w_warehouse_id_sku_id_by_move = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("move_count_max_qty").asc_nulls_last(), "id")
w_move_count_sku_id = Window.partitionBy("move_count", "sku_id")

filtered = (
    df_stock_moves
    .filter((~F.col("move_type").isin("INBOUND", "OUTBOUND")) & (F.col("unit_price") >= 130.6))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.round(F.col("unit_price") * 1.1, 2).alias("unit_price_adj"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.first_moved_at",
    )
)

with_windows = (
    with_counts
    .select(
        "*",
        F.max("qty").over(w_move_count).alias("move_count_max_qty"),
        F.min("unit_price_adj").over(w_move_count).alias("move_count_min_unit_price_adj"),
        F.dense_rank().over(w_all_by_qty_ties).alias("qty_dense_rank"),
        F.round(F.avg("qty").over(w_all_by_qty_prior_2), 2).alias("qty_avg_2_prior"),
        F.max("qty").over(w_all_by_qty_prior_1).alias("qty_max_1_prior"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.ntile(10).over(w_warehouse_id_sku_id_by_move).alias("move_count_max_qty_decile"),
        F.round(F.avg("qty_max_1_prior").over(w_move_count_sku_id), 2).alias("move_count_sku_id_avg_qty_max_1_prior"),
        F.round(F.col("qty") - F.avg("qty").over(w_move_count_sku_id), 2).alias("qty_vs_move_count_sku_id_avg"),
        F.min("unit_price_adj").over(w_move_count_sku_id).alias("move_count_sku_id_min_unit_price_adj"),
        (F.col("qty") - F.lag("qty").over(w_warehouse_id_sku_id_by_move)).alias("qty_change"),
        F.count(F.lit(1)).over(w_move_count_sku_id).alias("rows_in_move_count_sku_id"),
    )
)

top_rows = (
    with_trends
    .filter(F.col("qty_dense_rank") <= 5)
    .drop("qty_dense_rank")
)

result = (
    top_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "move_type_prefix",
        "move_count",
        "move_count_max_qty",
        "move_count_min_unit_price_adj",
        "qty_avg_2_prior",
        "qty_max_1_prior",
        "move_count_max_qty_decile",
        "move_count_sku_id_avg_qty_max_1_prior",
        "qty_vs_move_count_sku_id_avg",
        "move_count_sku_id_min_unit_price_adj",
        "rows_in_move_count_sku_id",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.round(F.col("unit_price") / F.nullif(F.col("unit_price_adj"), F.lit(0)), 3).alias("unit_price_per_unit_price_adj"),
    )
    .orderBy(F.col("warehouse_id").asc_nulls_last())
)
