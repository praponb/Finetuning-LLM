from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter(F.col("unit_price") < 399.6)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator").isin("op_bee", "op_ann"), 1)).alias("move_operator_count"),
        F.count(F.when(F.col("operator") == "op_eve", 1)).alias("move_operator_count_2"),
    )
)

with_stats = (
    prepared.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        F.coalesce(F.col("st.move_operator_count_2"), F.lit(0)).alias("move_operator_count_2"),
    )
)

detail_rows_moves = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.move_operator_count_2",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
    )
)

qualified = (
    detail_rows_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") <= 330.0) & (F.col("st.moved_at") < F.lit("2025-02-24 09:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("sku_id") >= 3)
)

result = (
    qualified
    .groupingSets(
        [["move_operator_count_2", "warehouse_id"], ["move_operator_count_2"], ["warehouse_id"]],
        "move_operator_count_2", "warehouse_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("move_moved_at").alias("first_move_moved_at"),
        F.min("moved_at").alias("first_moved_at"),
    )
    .orderBy(
        F.col("move_operator_count_2").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
