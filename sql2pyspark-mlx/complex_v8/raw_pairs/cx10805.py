from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id_by_da = Window.partitionBy("move_type", "warehouse_id").orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type = Window.partitionBy("move_type")
w_move_type_warehouse_id_by_da_2 = Window.partitionBy("move_type", "warehouse_id").orderBy("days_since_moved_at", "id")
w_move_type_sku_id_by_days_sin = Window.partitionBy("move_type", "sku_id").orderBy("days_since_moved_at", "id")
w_dedup = Window.partitionBy("sku_id", "move_type").orderBy(F.col("move_type_max_days_since_moved_at").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc(), F.col("id").desc())

filtered_moves = (
    df_stock_moves
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

metrics = (
    filtered_moves
    .select(
        "*",
        F.last("days_since_moved_at", ignorenulls=True).over(w_move_type_warehouse_id_by_da).alias("last_known_days_since_moved_at"),
        F.max("days_since_moved_at").over(w_move_type).alias("move_type_max_days_since_moved_at"),
        F.round(F.col("days_since_moved_at") - F.avg("days_since_moved_at").over(w_move_type), 2).alias("days_since_moved_at_vs_move_type_avg"),
        F.min("days_since_moved_at").over(w_move_type).alias("move_type_min_days_since_moved_at"),
        F.lead("days_since_moved_at", 3).over(w_move_type_warehouse_id_by_da_2).alias("next3_days_since_moved_at"),
        F.lag("moved_at", 3).over(w_move_type_sku_id_by_days_sin).alias("prev3_moved_at"),
    )
)

latest = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

latest_2 = (
    latest
    .withColumn("row_num", F.row_number().over(w_dedup_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    latest_2.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") > 119.5) & (F.col("st.moved_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp")), "left_anti")
)

result = (
    filtered
    .groupBy("warehouse_id", "moved_at")
    .pivot("move_type", ["TRANSFER", "ADJUST", "INBOUND"])
    .agg(F.min("days_since_moved_at_vs_move_type_avg"))
    .orderBy("warehouse_id", "moved_at")
)
