from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

labeled = (
    valid
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
    )
)

result = (
    labeled
    .select("id", "unit_price", "greatest_unit_price_qty")
    .unpivot(
        ids=["id"],
        values=["unit_price", "greatest_unit_price_qty"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric_name")
)
