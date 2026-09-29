from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_moving_2 = Window.orderBy("qty", "id").rowsBetween(-2, Window.currentRow)
w_all_by_qty_moving_3 = Window.orderBy("qty", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

sequenced = (
    cleaned
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.round(F.avg("qty").over(w_all_by_qty_moving_2), 2).alias("qty_avg_3"),
        F.round(F.avg("qty").over(w_all_by_qty_moving_3), 2).alias("qty_avg_4"),
    )
)

result = (
    sequenced
    .select(
        "id",
        F.col("qty_avg_4").alias("m_qty_avg_4"),
        F.col("qty_avg_3").alias("m_qty_avg_3"),
    )
    .unpivot(
        ids=["id"],
        values=["m_qty_avg_4", "m_qty_avg_3"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
