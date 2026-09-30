from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("unit_price") <= 396.4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
    )
)

with_details_sku = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "sk.sku_code",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

exploded_detail = (
    with_details_sku.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") < 357), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.unit_price",
        "s.warehouse_capacity",
        "s.warehouse_wh_code",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "move_id",
        "move_qty",
        "warehouse_capacity",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["move_qty", "warehouse_capacity"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
