from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("sku_id") <= 13) & (F.col("moved_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
    )
)

scored = (
    cleaned
    .filter(F.col("qty") <= 387)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "moved_at_quarter",
        F.concat(F.col("move_type"), F.lit("#"), F.col("id").cast("string")).alias("move_type_ref"),
    )
)

result = (
    scored
    .select("id", "qty", "moved_at_quarter")
    .unpivot(
        ids=["id"],
        values=["qty", "moved_at_quarter"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
