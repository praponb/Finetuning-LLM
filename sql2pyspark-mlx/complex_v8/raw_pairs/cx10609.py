from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(~F.col("move_type").isin("ADJUST", "OUTBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

enriched_warehouse = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.city") == "Singapore")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.operator_prefix",
        "s.move_type_operator_label",
        "s.days_since_moved_at",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.category").alias("sku_category"),
    )
)

with_details_sku = (
    enriched_warehouse.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.operator_prefix",
        "s.move_type_operator_label",
        "s.days_since_moved_at",
        "s.warehouse_opened_on",
        "s.warehouse_wh_code",
        "s.sku_category",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.city").alias("warehouse_city_2"),
        F.col("w.opened_on").alias("warehouse_opened_on_2"),
    )
)

derived = (
    with_details_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "operator_prefix",
        "move_type_operator_label",
        "days_since_moved_at",
        "warehouse_opened_on",
        "sku_unit_cost",
        "sku_labels",
        "warehouse_city_2",
        "warehouse_opened_on_2",
        F.datediff(F.col("warehouse_opened_on"), F.col("warehouse_opened_on_2")).alias("days_warehouse_opened_on_2_to_warehouse_opened_o"),
        F.length(F.trim(F.col("warehouse_city_2"))).alias("warehouse_city_2_len"),
    )
)

result = (
    derived
    .select("id", "sku_unit_cost", "unit_price")
    .unpivot(
        ids=["id"],
        values=["sku_unit_cost", "unit_price"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
