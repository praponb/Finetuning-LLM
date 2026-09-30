from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("qty") > 99)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
    )
)

exploded_detail = (
    prepared_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price",
        "s.least_qty_unit_price",
        "s.moved_at_label",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        "st.move_type",
        F.col("st.operator").alias("move_operator"),
    )
)

detail_rows = (
    exploded_detail.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.unit_price",
        "s.moved_at_label",
        "s.move_id",
        "s.move_qty",
        "s.move_type",
        "s.move_operator",
        F.col("st.id").alias("move_id_2"),
        F.col("st.qty").alias("move_qty_2"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

summarized_rows = (
    detail_rows.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

tagged = (
    summarized_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "moved_at_label",
        "move_id",
        "move_qty",
        "move_type",
        "move_operator",
        "move_id_2",
        "move_qty_2",
        "move_moved_at",
        "move_count",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("move_moved_at")).alias("days_since_move_moved_at"),
    )
)

result = (
    tagged
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "moved_at_label",
        "move_id",
        "move_qty",
        "move_id_2",
        "move_moved_at",
        "move_count",
        F.lower(F.regexp_replace(F.trim(F.col("moved_at_label")), " ", "_")).alias("moved_at_label_slug"),
        F.when(F.col("days_since_move_moved_at") >= 595, "A").when(F.col("days_since_move_moved_at") >= 210, "B").otherwise("C").alias("days_since_move_moved_at_tier"),
    )
    .orderBy(F.col("qty").asc_nulls_last())
)
