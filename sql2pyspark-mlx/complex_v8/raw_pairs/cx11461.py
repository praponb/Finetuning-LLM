from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-02-24 08:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

result = (
    valid
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        F.concat(F.col("operator"), F.lit("#"), F.col("id").cast("string")).alias("operator_ref"),
    )
    .orderBy(F.col("id").desc())
)
