from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("unit_price") < 328.3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.category").alias("sku_category"),
    )
)

line_level = (
    combined_details.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
    )
)

enriched = (
    line_level
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "move_id",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

result = (
    enriched
    .select(
        "id",
        "move_type",
        "moved_at",
        "move_id",
        "days_since_moved_at",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
    )
    .orderBy(
        F.col("days_since_moved_at").desc_nulls_first(),
        F.col("moved_at_dayofweek").desc(),
    )
)
