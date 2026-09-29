from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("unit_price") < 396.4) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
    )
)

with_details_warehouse = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
    )
)

result = (
    with_details_warehouse
    .select("id", "unit_price", "sku_weight_kg")
    .unpivot(
        ids=["id"],
        values=["unit_price", "sku_weight_kg"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
