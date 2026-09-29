from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (~F.col("move_type").isin("OUTBOUND", "ADJUST")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type") == "TRANSFER", 1)).alias("move_move_type_count"),
    )
)

profiled = (
    with_parent.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "qty",
        "operator",
        "sku_unit_cost",
        "sku_labels",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
    .orderBy("sku_unit_cost", F.col("qty").asc_nulls_last(), "id")
    .limit(20)
)
