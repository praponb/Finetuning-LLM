from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id_by_qt = Window.partitionBy("move_type", "warehouse_id").orderBy("qty", "id").rowsBetween(-2, Window.currentRow)
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price = Window.orderBy("unit_price", "id")

recent = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-03-02").cast("date")) & (F.col("move_type") == "INBOUND"))
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

windowed = (
    recent
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        F.round(F.avg("unit_price").over(w_move_type_warehouse_id_by_qt), 2).alias("unit_price_avg_3"),
        F.min("qty").over(w_sku_id_move_type).alias("sku_id_move_type_min_qty"),
        F.count(F.lit(1)).over(w_all_by_unit_price_running).alias("running_count"),
        F.row_number().over(w_all_by_unit_price).alias("seq_no"),
    )
)

tagged = (
    windowed
    .withColumn("unit_price_avg_3_filled", F.coalesce(F.col("unit_price_avg_3"), F.lit(0.0)))
)

leaders = tagged.filter(F.col("seq_no") == 1)

result = (
    leaders
    .groupBy("seq_no", "warehouse_id")
    .pivot("move_type", ["OUTBOUND", "INBOUND"])
    .agg(F.sum("running_count"))
    .orderBy("seq_no", "warehouse_id")
)
