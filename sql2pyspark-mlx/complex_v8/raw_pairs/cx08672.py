from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter(F.col("qty") > 89)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
    )
)

with_details_warehouse = (
    cleaned_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    with_details_warehouse
    .groupingSets(
        [["warehouse_city", "warehouse_id"], ["warehouse_city"], ["warehouse_id"], []],
        "warehouse_city", "warehouse_id",
    )
    .agg(
        F.grouping("warehouse_city").alias("is_total_warehouse_city"),
        F.round(F.stddev("sku_unit_cost"), 2).alias("stddev_sku_unit_cost"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.percentile_approx("qty", 0.5).alias("median_qty"),
    )
    .orderBy(
        F.col("warehouse_city").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
