from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("move_type") == "TRANSFER") & (F.col("moved_at") >= F.lit("2025-01-19 12:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("qty") >= 372, "A").when(F.col("qty") >= 111, "B").otherwise("C").alias("qty_tier"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.when(F.col("qty") >= 415, "large").when(F.col("qty") >= 189, "medium").otherwise("small").alias("qty_tier_2"),
    )
)

tagged = (
    cleaned
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "qty_tier",
        "move_type_len",
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
        F.when(F.col("unit_price") >= 290.4, "HIGH").when(F.col("unit_price") >= 236.5, "MEDIUM").otherwise("LOW").alias("unit_price_tier"),
        F.date_trunc("quarter", F.col("moved_at")).alias("moved_at_quarter"),
    )
)

annotated = (
    tagged
    .select(
        "*",
        F.round(F.col("qty") / F.nullif(F.col("unit_price_adj"), F.lit(0)), 3).alias("qty_per_unit_price_adj"),
        F.round(F.col("move_type_len") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("move_type_len_per_qty"),
    )
)

scored = (
    annotated
    .select(
        "*",
        (F.floor(F.col("move_type_len") / 2) * 2).alias("move_type_len_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "qty_tier",
        "move_type_len",
        "unit_price_adj",
        "unit_price_tier",
        "move_type_len_per_qty",
        "move_type_len_bucket",
        F.least(F.col("unit_price_adj"), F.col("qty")).alias("least_unit_price_adj_qty"),
    )
    .orderBy("move_type", F.col("move_type_len").asc_nulls_last())
)
