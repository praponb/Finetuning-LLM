from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") >= 5) & (F.col("unit_price") >= 119.5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
    )
)

joined = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.days_since_moved_at",
        "s.operator_prefix",
        F.col("w.city").alias("warehouse_city"),
    )
)

qualified = (
    joined
    .crossJoin(joined.agg(F.avg("days_since_moved_at").alias("overall_avg_days_since_moved_at")))
    .filter(F.col("days_since_moved_at") > F.col("overall_avg_days_since_moved_at"))
    .drop("overall_avg_days_since_moved_at")
)

labeled = (
    qualified
    .select(
        "*",
        F.abs(F.col("days_since_moved_at") - 490).alias("days_since_moved_at_distance"),
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

exploded_detail = (
    labeled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.operator_prefix",
        "s.days_since_moved_at_distance",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "warehouse_id",
        "unit_price",
        "operator_prefix",
        "move_id",
        "move_unit_price",
        "move_warehouse_id",
        "move_qty",
        "move_moved_at",
        F.date_format(F.col("move_moved_at"), "yyyy-MM-dd HH:00").alias("move_moved_at_label"),
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
    )
    .orderBy(F.col("move_id").asc(), F.col("move_warehouse_id").desc())
)
