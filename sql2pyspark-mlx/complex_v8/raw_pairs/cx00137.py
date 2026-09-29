from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(7, 10))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.year(F.col("moved_at")).alias("moved_at_year"),
    )
)

screened = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") < 348) & (F.col("st.unit_price") < 290.4), "left_semi")
)

tagged = (
    screened
    .select(
        "*",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

result = (
    tagged
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "moved_at_year",
        "move_type_or_unknown",
        "move_type_clean",
        "qty_bucket",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
    .orderBy(F.col("unit_price").asc_nulls_last(), "warehouse_id", "id")
    .limit(20)
)
