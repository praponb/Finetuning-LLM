from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(7, 10)) & (F.col("qty") < 299))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.min("moved_at").alias("first_moved_at"))
)

summarized_rows = (
    cleaned_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.first_moved_at")
)

sku_id_stats = (
    summarized_rows
    .groupBy("sku_id")
    .agg(F.percentile_approx("qty", 0.5).alias("median_qty"))
)

outliers = (
    summarized_rows.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.qty") >= F.col("g.median_qty"))
    .select(
        "s.*",
        "g.median_qty",
        F.round(F.col("s.qty") - F.col("g.median_qty"), 2).alias("qty_minus_median"),
    )
)

screened = (
    outliers
    .crossJoin(outliers.agg(F.avg("median_qty").alias("overall_avg_median_qty")))
    .filter(F.col("median_qty") > F.col("overall_avg_median_qty"))
    .drop("overall_avg_median_qty")
)

result = (
    screened
    .groupingSets(
        [["warehouse_id", "sku_id", "move_type"], ["warehouse_id"], ["sku_id"], []],
        "warehouse_id", "sku_id", "move_type",
    )
    .agg(
        F.round(F.sum("median_qty") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("median_qty_to_qty_ratio"),
        F.round(F.sum("median_qty") / F.nullif(F.sum("qty_minus_median"), F.lit(0)), 3).alias("median_qty_to_qty_minus_median_ratio"),
        F.round(F.sum("median_qty"), 2).alias("total_median_qty"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
