from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), F.col("id").desc())

scoped = (
    df_stock_moves
    .filter(F.col("qty").between(123, 394))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id") >= 5), "left_semi")
    .filter(F.col("operator") == "op_ann")
)

deduped = (
    without_matches
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

warehouse_id_stats = (
    deduped
    .groupBy("warehouse_id")
    .agg(F.avg("days_since_moved_at").alias("avg_days_since_moved_at"))
)

outliers = (
    deduped.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.days_since_moved_at") > F.col("g.avg_days_since_moved_at"))
    .select(
        "s.*",
        "g.avg_days_since_moved_at",
        F.round(F.col("s.days_since_moved_at") - F.col("g.avg_days_since_moved_at"), 2).alias("days_since_moved_at_minus_avg"),
    )
)

scored = (
    outliers
    .filter(F.col("days_since_moved_at") < 560)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "days_since_moved_at",
        "avg_days_since_moved_at",
        F.least(F.col("unit_price"), F.col("avg_days_since_moved_at")).alias("least_unit_price_avg_days_since_moved_at"),
    )
)

result = (
    scored
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "avg_days_since_moved_at",
        "least_unit_price_avg_days_since_moved_at",
    )
    .orderBy(
        F.col("move_type").asc(),
        F.col("least_unit_price_avg_days_since_moved_at").asc(),
    )
)
