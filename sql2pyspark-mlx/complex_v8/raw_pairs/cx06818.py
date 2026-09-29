from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_sku_id_running = Window.partitionBy("sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_max_session_no = Window.partitionBy("max_session_no")
w_last_moved_at_year_total_sta = Window.partitionBy(F.year("last_moved_at"), "total_starts_session").orderBy(F.col("min_session_no").desc(), "move_type")

cleaned = (
    df_stock_moves
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

with_prev = cleaned.withColumn("prev_moved_at", F.lag("moved_at").over(w_sku_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_sku_id_running))

grouped = (
    sessionized
    .groupBy("move_type")
    .agg(
        F.sum("starts_session").alias("total_starts_session"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.max("session_no").alias("max_session_no"),
        F.min("session_no").alias("min_session_no"),
        F.max("moved_at").alias("last_moved_at"),
        F.round(F.sum("qty") / F.nullif(F.sum("session_no"), F.lit(0)), 3).alias("qty_to_session_no_ratio"),
        F.sum("unit_price").alias("sum_unit_price_check"),
    )
    .filter(F.col("sum_unit_price_check") > 150.7)
    .drop("sum_unit_price_check")
)

ranked = (
    grouped
    .select(
        "move_type",
        "total_starts_session",
        "avg_session_no",
        "max_session_no",
        "min_session_no",
        "last_moved_at",
        "qty_to_session_no_ratio",
        F.round(F.col("avg_session_no") / F.nullif(F.sum("avg_session_no").over(w_max_session_no), F.lit(0)), 4).alias("avg_session_no_share_of_max_session_no"),
        F.max("avg_session_no").over(w_max_session_no).alias("max_session_no_max_avg_session_no"),
        F.round(F.percent_rank().over(w_last_moved_at_year_total_sta), 3).alias("min_session_no_percent_rank"),
    )
)

result = (
    ranked
    .select(
        "move_type",
        "total_starts_session",
        "avg_session_no",
        "max_session_no",
        "min_session_no",
        "last_moved_at",
        "qty_to_session_no_ratio",
        "max_session_no_max_avg_session_no",
        F.when(F.col("move_type").isin("ADJUST", "INBOUND"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_moved_at")).alias("days_since_last_moved_at"),
    )
    .orderBy(F.col("min_session_no").desc_nulls_first())
)
