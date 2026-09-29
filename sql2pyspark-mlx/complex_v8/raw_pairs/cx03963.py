from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter(F.col("unit_price") <= 387.1)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

combined_details = (
    cleaned_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

result = (
    combined_details
    .select("id", "sku_weight_kg", "sku_unit_cost")
    .unpivot(
        ids=["id"],
        values=["sku_weight_kg", "sku_unit_cost"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .orderBy("id", "metric")
)
