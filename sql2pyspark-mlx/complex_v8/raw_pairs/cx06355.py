from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 5)
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
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
        F.when(F.col("move_type").isin("OUTBOUND", "ADJUST"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)

with_details = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        "s.move_type_len",
        "s.moved_at_year",
        "s.move_type_group",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

filtered = (
    with_details.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.to_date(F.col("st.moved_at")) < F.lit("2025-02-18").cast("date")), "left_semi")
)

joined = (
    filtered.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        "s.move_type_len",
        "s.moved_at_year",
        "s.move_type_group",
        "s.warehouse_wh_code",
        F.col("w.capacity").alias("warehouse_capacity_2"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code_2"),
    )
)

line_level_moves = (
    joined.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (~F.col("st.move_type").isin("INBOUND", "ADJUST")), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.move_type_len",
        "s.move_type_group",
        "s.warehouse_wh_code",
        "s.warehouse_capacity_2",
        "s.warehouse_wh_code_2",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.qty").alias("move_qty"),
    )
)

enriched = (
    line_level_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.move_type_group",
        "s.warehouse_wh_code",
        "s.warehouse_wh_code_2",
        "s.move_id",
        "s.move_move_type",
        "s.move_warehouse_id",
        "s.move_qty",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    enriched
    .select(
        "id",
        "move_id",
        "sku_weight_kg",
        "sku_unit_cost",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["sku_weight_kg", "sku_unit_cost"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
