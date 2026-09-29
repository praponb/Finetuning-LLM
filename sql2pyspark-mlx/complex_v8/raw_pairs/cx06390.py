from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("move_type").orderBy(F.col("moved_at").desc(), F.col("id").desc())

scoped_moves = (
    df_stock_moves
    .filter((F.col("operator").isNotNull()) & (F.col("moved_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

deduped = (
    scoped_moves
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

warehouse_id_stats = (
    deduped
    .groupBy("warehouse_id")
    .agg(F.percentile_approx("unit_price", 0.5).alias("median_unit_price"))
)

vs_group = (
    deduped.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.unit_price") >= F.col("g.median_unit_price"))
    .select(
        "s.*",
        "g.median_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.median_unit_price"), 2).alias("unit_price_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "id",
        F.col("days_since_moved_at").alias("m_days_since_moved_at"),
        F.col("qty").alias("m_qty"),
    )
    .unpivot(
        ids=["id"],
        values=["m_days_since_moved_at", "m_qty"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
