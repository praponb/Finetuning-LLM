from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_category_by_max_move_qty = Window.partitionBy("sku_category").orderBy("max_move_qty", "id")
w_all_by_max_move_qty = Window.orderBy("max_move_qty", "id")
w_sku_category_by_max_move_qty_2 = Window.partitionBy("sku_category").orderBy("max_move_qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id = Window.partitionBy("sku_id")
w_all_by_max_move_qty_running = Window.orderBy("max_move_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_max_move_qty_full = Window.orderBy("max_move_qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_moves = (
    df_stock_moves
    .filter((F.col("operator").isNotNull()) & (F.col("warehouse_id") <= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

enriched_sku = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.least_unit_price_qty",
        "s.moved_at_plus_7d",
        "s.operator_move_type_label",
        F.col("sk.category").alias("sku_category"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.max("qty").alias("max_move_qty"))
)

with_counts = (
    enriched_sku.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.max_move_qty")
)

annotated = (
    with_counts
    .select(
        "*",
        (F.col("unit_price") + F.col("least_unit_price_qty")).alias("unit_price_plus_least_unit_price_qty"),
    )
)

with_windows = (
    annotated
    .select(
        "*",
        F.lead("unit_price", 2).over(w_sku_category_by_max_move_qty).alias("next2_unit_price"),
        F.ntile(3).over(w_all_by_max_move_qty).alias("max_move_qty_tercile"),
        F.nth_value("max_move_qty", 2).over(w_sku_category_by_max_move_qty_2).alias("second_max_move_qty"),
        F.min("unit_price").over(w_sku_id).alias("sku_id_min_unit_price"),
        F.sum("least_unit_price_qty").over(w_all_by_max_move_qty_running).alias("running_least_unit_price_qty"),
        F.first("least_unit_price_qty").over(w_all_by_max_move_qty_full).alias("first_least_unit_price_qty"),
    )
)

result = (
    with_windows
    .rollup("max_move_qty_tercile", "sku_category")
    .agg(
        F.grouping("max_move_qty_tercile").alias("is_total_max_move_qty_tercile"),
        F.round(F.sum("max_move_qty_tercile") / F.nullif(F.sum("max_move_qty"), F.lit(0)), 3).alias("max_move_qty_tercile_to_max_move_qty_ratio"),
    )
)
