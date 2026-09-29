from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter(F.col("operator").isin("op_bee", "op_eve", "op_chan"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
        F.concat_ws(": ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

tagged = (
    cleaned_moves
    .select(
        "*",
        F.abs(F.col("unit_price") - 293.2).alias("unit_price_distance"),
        F.lower(F.trim(F.col("operator_move_type_label"))).alias("operator_move_type_label_clean"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

joined_sku = (
    tagged.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.upper(F.trim(F.col("sk.sku_code"))).like("A%")), "inner")
    .filter(F.col("sk.unit_cost") < 302.5)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.moved_at_label",
        "s.operator_move_type_label_clean",
        "s.unit_price_plus_qty",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.max("moved_at").alias("last_moved_at"),
        F.sum("unit_price").alias("total_move_unit_price"),
    )
)

profiled = (
    joined_sku.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.last_moved_at",
        "st.total_move_unit_price",
    )
)

result = (
    profiled
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "moved_at_label",
        "unit_price_plus_qty",
        "last_moved_at",
        "total_move_unit_price",
        F.date_format(F.col("last_moved_at"), "yyyy-MM").alias("last_moved_at_label"),
    )
    .orderBy("operator")
)
