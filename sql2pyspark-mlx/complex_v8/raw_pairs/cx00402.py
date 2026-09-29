from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("days_since_moved_at").desc(), F.col("id").desc())

cleaned = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
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
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at_2"),
    )
)

unique_rows = (
    cleaned
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

stats = (
    unique_rows
    .groupBy("sku_id")
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.sum("qty").alias("total_qty"),
        F.max("moved_at").alias("last_moved_at"),
        F.sum(F.when(F.col("operator").isNotNull(), 1).otherwise(0)).alias("n_operator_is_not_null"),
        F.max("days_since_moved_at_2").alias("max_days_since_moved_at_2_check"),
    )
    .filter(F.col("max_days_since_moved_at_2_check") > 210)
    .drop("max_days_since_moved_at_2_check")
)

result = (
    stats
    .select("sku_id", "n_operator_is_not_null", "total_qty")
    .unpivot(
        ids=["sku_id"],
        values=["n_operator_is_not_null", "total_qty"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("sku_id", "metric")
)
