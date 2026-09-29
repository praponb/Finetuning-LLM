from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_all_by_moved_at_full = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_all_by_moved_at_running = Window.orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_moves = (
    df_stock_moves
    .filter(F.col("move_type") != "OUTBOUND")
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

annotated = (
    prepared_moves
    .select(
        "*",
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
    )
)

ranked = (
    annotated
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "unit_price_adj",
        "move_type_prefix",
        "moved_at_label",
        F.max("unit_price").over(w_move_type_warehouse_id).alias("move_type_warehouse_id_max_unit_price"),
        F.last("qty").over(w_all_by_moved_at_full).alias("last_qty"),
        F.lag("unit_price").over(w_all_by_moved_at).alias("prev_unit_price"),
        F.last("unit_price").over(w_all_by_moved_at_full).alias("last_unit_price"),
        F.last("qty", ignorenulls=True).over(w_all_by_moved_at_running).alias("last_known_qty"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"))
)

with_stats = (
    ranked.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.avg_move_unit_price")
)

result = (
    with_stats
    .select(
        "id",
        "move_type_warehouse_id_max_unit_price",
        "unit_price",
        "avg_move_unit_price",
        "last_unit_price",
    )
    .unpivot(
        ids=["id"],
        values=["move_type_warehouse_id_max_unit_price", "unit_price", "avg_move_unit_price", "last_unit_price"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric_name")
)
