from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_plus_qty = Window.orderBy(F.col("unit_price_plus_qty").desc(), "id")
w_all_by_move_count_full = Window.orderBy("move_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_stock_moves
    .filter((F.col("qty") <= 415) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"), F.count("*").alias("move_count_2"))
)

with_counts = (
    valid.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

enriched = (
    with_counts
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "moved_at_hour",
        "moved_at_label",
        "move_count",
        "move_count_2",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label_2"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "moved_at_hour",
        "moved_at_label",
        "move_count",
        "move_count_2",
        "unit_price_plus_qty",
        F.round(F.cume_dist().over(w_all_by_unit_price_plus_qty), 3).alias("unit_price_plus_qty_cume_dist"),
        F.nth_value("unit_price_plus_qty", 2).over(w_all_by_move_count_full).alias("second_unit_price_plus_qty"),
    )
)

result = (
    ranked
    .groupBy("sku_id")
    .pivot("operator", ["op_eve", "op_chan", "op_dew", "op_bee"])
    .agg(F.min("unit_price_plus_qty_cume_dist").alias("min_v"), F.sum("unit_price_plus_qty_cume_dist").alias("sum_v"))
)
