from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_qty = Window.orderBy("total_qty", "move_type", "warehouse_id")
w_all_by_total_qty_running = Window.orderBy(F.col("total_qty").desc(), "move_type", "warehouse_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_total_qty_full = Window.orderBy("total_qty", "move_type", "warehouse_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
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

with_activity = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

filtered_2 = (
    with_activity.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") > 191), "left_semi")
)

summary = (
    filtered_2
    .filter(F.col("move_type") == "ADJUST")
    .groupBy("move_type", "warehouse_id")
    .agg(
        F.sum("qty").alias("total_qty"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
    )
)

sequenced = (
    summary
    .select(
        "*",
        F.lag("total_qty", 3).over(w_all_by_total_qty).alias("prev3_total_qty"),
        F.ntile(5).over(w_all_by_total_qty).alias("total_qty_quintile"),
        F.min("total_qty").over(w_all_by_total_qty_running).alias("min_total_qty_so_far"),
        F.last("total_qty").over(w_all_by_total_qty_full).alias("last_total_qty"),
    )
)

without_matches = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator") != "op_eve") & (F.to_date(F.col("st.moved_at")) < F.lit("2025-03-08").cast("date")), "left_anti")
)

result = (
    without_matches
    .filter(F.size(F.col("move_type_list")) >= 1)
    .select(
        "move_type",
        "total_qty",
        "prev3_total_qty",
        "total_qty_quintile",
        "min_total_qty_so_far",
        "last_total_qty",
    )
    .orderBy(F.col("move_type").asc())
)
