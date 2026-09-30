from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter((~F.col("move_type").isin("OUTBOUND", "ADJUST")) & (F.col("sku_id") <= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
    )
)

tagged = (
    prepared
    .select(
        "id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

filtered = (
    tagged.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") <= 330.9) & (F.col("st.qty") < 275), "left_semi")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count(F.when(F.col("operator") != "op_eve", 1)).alias("move_operator_count"),
    )
)

profiled = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "days_since_moved_at",
        "last_moved_at",
        "move_operator_count",
    )
    .orderBy(F.col("days_since_moved_at").desc(), F.col("move_type").desc())
)
