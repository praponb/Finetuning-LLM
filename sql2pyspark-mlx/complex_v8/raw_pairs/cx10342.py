from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("qty") < 392))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

labeled = (
    valid
    .filter(F.col("qty") < 387)
    .select(
        "*",
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
    )
)

enriched = (
    labeled
    .select(
        "*",
        F.lower(F.trim(F.col("move_type_ref"))).alias("move_type_ref_clean"),
    )
)

enriched_2 = (
    enriched.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price",
        "s.operator_clean",
        "s.move_type_ref",
        "s.move_type_ref_clean",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

enriched_3 = (
    enriched_2.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.operator_clean",
        "s.move_type_ref",
        "s.move_type_ref_clean",
        "s.sku_category",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code_2"),
        F.col("w.city").alias("warehouse_city"),
    )
)

detail_rows = (
    enriched_3.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.operator_clean",
        "s.move_type_ref",
        "s.move_type_ref_clean",
        "s.warehouse_capacity",
        "s.warehouse_wh_code_2",
        "s.warehouse_city",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

labeled_2 = (
    detail_rows
    .withColumn("warehouse_capacity_filled", F.coalesce(F.col("warehouse_capacity"), F.lit(0)))
)

result = (
    labeled_2
    .select(
        "id",
        "sku_id",
        "unit_price",
        "operator_clean",
        "move_type_ref",
        "move_type_ref_clean",
        "warehouse_capacity",
        "warehouse_wh_code_2",
        "move_moved_at",
        "move_sku_id",
        "warehouse_capacity_filled",
        F.greatest(F.col("unit_price"), F.col("warehouse_capacity")).alias("greatest_unit_price_warehouse_capacity"),
        F.lower(F.regexp_replace(F.trim(F.col("move_operator")), " ", "_")).alias("move_operator_slug"),
    )
)
