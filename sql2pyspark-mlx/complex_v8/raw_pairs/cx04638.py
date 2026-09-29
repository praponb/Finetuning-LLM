from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

with_prev = prepared.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_warehouse_id_running))

scored = (
    sessionized
    .filter(F.hour(F.col("moved_at")).between(10, 14))
    .select(
        "*",
        F.round(F.col("burst_id") / F.nullif(F.col("days_since_moved_at"), F.lit(0)), 3).alias("burst_id_per_days_since_moved_at"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

result = (
    scored
    .filter(F.col("move_type_len") < 10)
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "days_since_moved_at",
        "prev_moved_at",
        "starts_session",
        "burst_id_per_days_since_moved_at",
        "move_type_len",
        "move_type_clean",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev_moved_at")).alias("days_since_prev_moved_at"),
        F.when(F.col("operator").isin("op_bee", "op_ann"), "group_a").when(F.col("operator") == "op_chan", "group_b").otherwise("other").alias("operator_group"),
    )
    .orderBy(F.col("move_type").asc_nulls_last(), "move_type_len")
)
