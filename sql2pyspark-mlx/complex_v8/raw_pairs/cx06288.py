from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

stats = (
    scoped_moves
    .groupBy("sku_id", "operator", "move_type_len")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.percentile_approx("qty_per_unit_price", 0.5).alias("median_qty_per_unit_price"),
        F.sum("qty_per_unit_price").alias("sum_qty_per_unit_price_check"),
    )
    .filter(F.col("sum_qty_per_unit_price_check") > 3.0)
    .drop("sum_qty_per_unit_price_check")
)

qualified = (
    stats.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.unit_cost") < 251.2) & (~F.col("sk.category").isin("Electronics", "Food")), "left_semi")
)

exploded_detail = (
    qualified.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.sku_id",
        "s.operator",
        "s.move_type_len",
        "s.stddev_unit_price",
        "s.median_qty_per_unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
        "st.move_type",
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    exploded_detail
    .filter(F.col("move_id") >= 56)
    .select(
        "sku_id",
        "operator",
        "move_type_len",
        "stddev_unit_price",
        "median_qty_per_unit_price",
        "move_id",
        "move_qty",
        "move_moved_at",
        "move_type",
        (F.floor(F.col("move_type_len") / 2) * 2).alias("move_type_len_bucket"),
        (F.floor(F.col("move_type_len") / 2) * 2).alias("move_type_len_bucket_2"),
    )
)
