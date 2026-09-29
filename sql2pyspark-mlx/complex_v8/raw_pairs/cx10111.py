from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_avg_qty_ties = Window.orderBy("avg_qty")
w_move_count = Window.partitionBy("move_count")
w_all_by_avg_qty = Window.orderBy("avg_qty", "move_count")

recent_moves = (
    df_stock_moves
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

profiled = (
    recent_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

summary = (
    profiled
    .groupBy("move_count")
    .agg(
        F.round(F.sum("move_count") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("move_count_to_qty_ratio"),
        F.round(100 * F.count(F.when(F.col("move_type") != "INBOUND", 1)) / F.count("*"), 1).alias("pct_move_type_inbound"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.max("qty").alias("max_qty"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.sum("move_count").alias("sum_move_count_check"),
    )
    .filter(F.col("sum_move_count_check") > 2)
    .drop("sum_move_count_check")
)

sequenced = (
    summary
    .select(
        "move_count",
        "move_count_to_qty_ratio",
        "pct_move_type_inbound",
        "distinct_move_types",
        "max_qty",
        F.dense_rank().over(w_all_by_avg_qty_ties).alias("avg_qty_dense_rank"),
        F.count(F.lit(1)).over(w_move_count).alias("rows_in_move_count"),
        F.lag("distinct_sku_ids", 2).over(w_all_by_avg_qty).alias("prev2_distinct_sku_ids"),
        F.round(F.col("avg_qty") / F.nullif(F.sum("avg_qty").over(w_move_count), F.lit(0)), 4).alias("avg_qty_share_of_move_count"),
    )
)

result = (
    sequenced
    .filter(F.col("avg_qty_share_of_move_count") > 0.2)
    .select(
        "move_count",
        "move_count_to_qty_ratio",
        "pct_move_type_inbound",
        "avg_qty_dense_rank",
        "rows_in_move_count",
        "prev2_distinct_sku_ids",
        "avg_qty_share_of_move_count",
        (F.floor(F.col("rows_in_move_count") / 5) * 5).alias("rows_in_move_count_bucket"),
    )
    .orderBy(F.col("pct_move_type_inbound").desc_nulls_first())
)
