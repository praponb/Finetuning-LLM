from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-03-08").cast("date")) & (F.col("warehouse_id").isNotNull()))
)

scored = (
    cleaned
    .select(
        "*",
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

result = (
    scored
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
