from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 3) & (F.col("unit_price") < 330.9))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

with_parent_sku = (
    valid.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category").isin("Electronics", "Beauty", "Toys")), "inner")
    .filter(F.col("sk.unit_cost") >= 38.3)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.qty_filled",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    with_parent_sku
    .rollup("warehouse_id", "sku_id")
    .agg(
        F.grouping("warehouse_id").alias("is_total_warehouse_id"),
        F.percentile_approx("qty", 0.5).alias("median_qty"),
        F.min("sku_unit_cost").alias("min_sku_unit_cost"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
