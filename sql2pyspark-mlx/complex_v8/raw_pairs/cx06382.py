from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter((F.col("unit_price") < 367.7) & (F.col("sku_id") >= 15))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
        F.when(F.col("operator").isin("op_bee", "op_chan"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group"),
    )
)

scored = (
    prepared_moves
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.abs(F.col("unit_price_plus_qty") - 400.6).alias("unit_price_plus_qty_distance"),
    )
)

line_level_moves = (
    scored.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.unit_price_plus_qty",
        "s.unit_price_plus_qty_distance",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

with_details = (
    line_level_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.opened_on").isNotNull()), "inner")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "left")
    .filter(F.col("w.opened_on") < F.lit("2025-02-14").cast("date"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.unit_price_plus_qty",
        "s.move_id",
        "s.move_qty",
        "s.move_sku_id",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "move_qty",
        "move_sku_id",
        "warehouse_wh_code",
        "warehouse_city",
        "warehouse_opened_on",
        "move_unit_price",
        F.when(F.col("move_unit_price") >= 341.5, "large").when(F.col("move_unit_price") >= 119.5, "medium").otherwise("small").alias("move_unit_price_tier"),
    )
)
