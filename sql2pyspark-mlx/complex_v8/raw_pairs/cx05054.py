from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.when(F.col("operator").isin("op_eve", "op_dew"), "group_a").when(F.col("operator") == "op_chan", "group_b").otherwise("other").alias("operator_group"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at_date",
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

joined_sku = (
    joined.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.wh_code").alias("warehouse_wh_code_2"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

result = (
    joined_sku
    .select(
        "id",
        "sku_id",
        "move_type",
        "warehouse_wh_code",
        "sku_unit_cost",
        "sku_is_active",
        "warehouse_capacity",
        F.when(F.col("sku_unit_cost") >= 320.4, "large").when(F.col("sku_unit_cost") >= 68.0, "medium").otherwise("small").alias("sku_unit_cost_tier"),
    )
    .orderBy(
        F.col("sku_unit_cost").desc_nulls_first(),
        F.col("move_type").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
