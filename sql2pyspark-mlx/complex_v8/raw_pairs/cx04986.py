from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id").orderBy("days_since_moved_at", "id")

scoped_moves = (
    df_stock_moves
    .filter((F.col("move_type") == "ADJUST") & (F.col("qty").between(61, 326)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

deduped = (
    scoped_moves
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_activity = (
    deduped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.month(F.col("st.moved_at")).isin(2, 3)), "left_anti")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.count(F.when(F.col("move_type") != "ADJUST", 1)).alias("move_move_type_count"),
    )
)

with_counts = (
    with_activity.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

result = (
    with_counts
    .select("id", "qty", "days_since_moved_at")
    .unpivot(
        ids=["id"],
        values=["qty", "days_since_moved_at"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
