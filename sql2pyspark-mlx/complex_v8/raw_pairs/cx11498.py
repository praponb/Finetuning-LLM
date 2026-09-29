from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator", "move_type").orderBy(F.col("unit_price").desc(), F.col("id").desc())
w_sku_id_moved_at_dayofweek_by = Window.partitionBy("sku_id", "moved_at_dayofweek").orderBy("unit_price", "id")
w_sku_id = Window.partitionBy("sku_id")
w_unit_price_seq_by_unit_price = Window.partitionBy("unit_price_seq").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp")) & (F.col("operator").isin("op_bee", "op_ann")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

latest = (
    cleaned
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    latest
    .withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id_moved_at_dayofweek_by))
    .withColumn("unit_price_seq", F.row_number().over(w_sku_id_moved_at_dayofweek_by))
)

tagged = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "moved_at_dayofweek",
        "prev_moved_at",
        "unit_price_seq",
        F.when(F.col("move_type").isin("TRANSFER", "ADJUST"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.sum("qty").alias("total_move_qty"))
)

with_stats = (
    tagged.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.total_move_qty")
)

with_windows = (
    with_stats
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "moved_at_dayofweek",
        "prev_moved_at",
        "unit_price_seq",
        "move_type_group",
        "total_move_qty",
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id), 2).alias("unit_price_vs_sku_id_avg"),
        F.max("unit_price_seq").over(w_sku_id).alias("sku_id_max_unit_price_seq"),
        F.first("total_move_qty", ignorenulls=True).over(w_unit_price_seq_by_unit_price).alias("first_known_total_move_qty"),
    )
)

summary = (
    with_windows
    .filter(F.col("first_known_total_move_qty") < 1650)
    .groupBy("unit_price_seq")
    .agg(
        F.round(F.sum("total_move_qty") / F.nullif(F.sum("first_known_total_move_qty"), F.lit(0)), 3).alias("total_move_qty_to_first_known_total_move_qty_rat"),
        F.count("*").alias("n_rows"),
        F.max("total_move_qty").alias("max_total_move_qty"),
        F.sum("unit_price_seq").alias("total_unit_price_seq"),
        F.min("first_known_total_move_qty").alias("min_first_known_total_move_qty"),
    )
)

result = (
    summary
    .select(
        "unit_price_seq",
        "n_rows",
        "max_total_move_qty",
        "min_first_known_total_move_qty",
        F.abs(F.col("max_total_move_qty") - 1650).alias("max_total_move_qty_distance"),
    )
    .orderBy(F.col("unit_price_seq").desc(), "n_rows")
)
