from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-01-13 13:00:00").cast("timestamp")) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.year(F.col("moved_at")).alias("moved_at_year"),
    )
)

screened = (
    base.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.opened_on").isNotNull()), "left_anti")
)

combined_details_warehouse = (
    screened.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        "s.move_type_len",
        "s.moved_at_year",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    combined_details_warehouse
    .groupingSets(
        [["warehouse_id", "move_type_len", "sku_id"], ["warehouse_id"], ["move_type_len"], []],
        "warehouse_id", "move_type_len", "sku_id",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.countDistinct("move_type").alias("distinct_move_types"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("move_type_len").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
