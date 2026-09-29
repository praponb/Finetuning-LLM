from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(10, 15)) & (F.col("move_type") == "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

derived = (
    prepared
    .filter(F.col("qty_per_unit_price") <= 7.5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "qty_per_unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("id").cast("string")).alias("operator_ref"),
    )
)

result = (
    derived
    .groupingSets(
        [["warehouse_id", "sku_id"], ["warehouse_id"]],
        "warehouse_id", "sku_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("qty_per_unit_price").alias("max_qty_per_unit_price"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
