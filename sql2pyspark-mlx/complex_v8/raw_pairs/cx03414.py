from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_unit_price = Window.partitionBy("operator").orderBy(F.col("unit_price").asc_nulls_last(), "id")
w_operator_by_unit_price_full = Window.partitionBy("operator").orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.row_number().over(w_operator_by_unit_price).alias("unit_price_seq"),
        F.lead("unit_price_filled", 2, 0).over(w_operator_by_unit_price).alias("next2_unit_price_filled"),
        F.nth_value("unit_price_filled", 2).over(w_operator_by_unit_price_full).alias("second_unit_price_filled"),
        F.lead("unit_price", 1, 0).over(w_operator_by_unit_price).alias("next_unit_price"),
    )
)

annotated = (
    with_windows
    .filter(F.col("unit_price") >= 189.4)
    .select(
        "*",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.min("moved_at").alias("first_moved_at"),
        F.min("qty").alias("min_move_qty"),
    )
)

summarized_rows = (
    annotated.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.first_moved_at",
        "st.min_move_qty",
    )
)

result = (
    summarized_rows
    .cube("unit_price_seq", "move_count", "sku_id")
    .agg(F.countDistinct("operator").alias("distinct_operators"))
    .orderBy(
        F.col("unit_price_seq").asc_nulls_last(),
        F.col("move_count").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
