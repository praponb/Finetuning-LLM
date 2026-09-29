from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.col("unit_price") >= 161.4) & (F.col("qty") > 93))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.round(F.avg("qty"), 2).alias("avg_move_qty"),
        F.count("*").alias("move_count"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        "st.avg_move_qty",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        F.col("move_count").alias("move_count_v"),
        F.col("qty_bucket").alias("qty_bucket_v"),
    )
    .unpivot(
        ids=["id"],
        values=["move_count_v", "qty_bucket_v"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
