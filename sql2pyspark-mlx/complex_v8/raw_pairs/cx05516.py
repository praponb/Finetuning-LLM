from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.col("operator") == "op_eve")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

with_activity = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") > 116), "left_anti")
)

result = (
    with_activity
    .cube("move_type", "warehouse_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("qty_bucket") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("qty_bucket_to_qty_ratio"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "warehouse_id",
        "grouping_level",
        "qty_bucket_to_qty_ratio",
        "avg_qty",
        "row_count",
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
