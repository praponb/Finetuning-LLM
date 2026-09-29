from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter(F.col("qty").between(90, 387))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 367.7, "HIGH").when(F.col("unit_price") >= 215.4, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
        F.concat_ws("-", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

without_matches = (
    recent_moves
    .crossJoin(recent_moves.agg(F.max("unit_price").alias("overall_max_unit_price")))
    .filter(F.col("unit_price") < F.col("overall_max_unit_price"))
    .drop("overall_max_unit_price")
)

result = (
    without_matches
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_tier",
        "operator_slug",
        F.length(F.trim(F.col("unit_price_tier"))).alias("unit_price_tier_len"),
    )
    .orderBy(F.col("warehouse_id").desc(), "operator_slug", "id")
    .limit(10)
)
