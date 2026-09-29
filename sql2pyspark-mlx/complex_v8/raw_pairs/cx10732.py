from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.year(F.col("moved_at")).alias("moved_at_year"),
        F.year(F.col("moved_at")).alias("moved_at_year_2"),
    )
)

with_children = (
    base.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.days_since_moved_at",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
    )
)

with_details = (
    with_children.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select("s.*", F.col("sk.unit_cost").alias("sku_unit_cost"))
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.min("qty").alias("min_move_qty"),
        F.round(F.avg("qty"), 2).alias("avg_move_qty"),
    )
)

profiled = (
    with_details.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        "st.min_move_qty",
        "st.avg_move_qty",
    )
)

result = (
    profiled
    .filter(F.col("move_id") >= 6)
    .select(
        "id",
        "sku_id",
        "move_type",
        "days_since_moved_at",
        "move_id",
        "move_move_type",
        "move_qty",
        "sku_unit_cost",
        "min_move_qty",
        "avg_move_qty",
        F.when(F.col("move_move_type").isin("TRANSFER", "INBOUND"), "group_a").when(F.col("move_move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_move_type_group"),
    )
)
