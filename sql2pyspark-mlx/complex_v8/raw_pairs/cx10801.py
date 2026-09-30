from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

with_details = (
    cleaned_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    with_details
    .filter(F.col("qty") < 349)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "unit_price",
        "warehouse_city",
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
        F.concat_ws("|", F.col("operator"), F.col("warehouse_city")).alias("operator_warehouse_city_label"),
    )
)
