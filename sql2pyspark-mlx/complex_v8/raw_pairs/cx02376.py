from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("sku_id") <= 4) | (F.col("move_type") == "OUTBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

screened = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") < 11506) & (F.col("w.city") == "Kuala Lumpur"), "left_semi")
)

with_parent_warehouse = (
    screened.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity").between(2614, 11768)), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.move_type_clean",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

result = (
    with_parent_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "move_type_clean",
        "warehouse_wh_code",
        "sku_unit_cost",
        "sku_weight_kg",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
    .orderBy("operator_slug", F.col("id").desc())
)
