from pyspark.sql import functions as F

base = (
    df_warehouses
    .filter(~F.col("city").isin("Kuala Lumpur", "Singapore"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

exploded_detail_moves = (
    base.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.capacity",
        "s.opened_on",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
        "st.move_type",
    )
)

exploded_detail = (
    exploded_detail_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.capacity",
        "s.opened_on",
        "s.move_id",
        "s.move_qty",
        "s.move_operator",
        "s.move_unit_price",
        "s.move_type",
        F.col("st.id").alias("move_id_2"),
        F.col("st.qty").alias("move_qty_2"),
        F.col("st.operator").alias("move_operator_2"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

grouped = (
    exploded_detail
    .groupBy(
        "move_operator_2",
        F.date_format("opened_on", "yyyy-MM").alias("opened_on_ym"),
    )
    .agg(
        F.sum(F.when(F.col("move_type").isNotNull(), 1).otherwise(0)).alias("n_move_type_is_not_null"),
        F.round(F.avg("capacity"), 2).alias("avg_capacity"),
        F.round(F.sum("move_qty") / F.nullif(F.sum("move_qty_2"), F.lit(0)), 3).alias("move_qty_to_move_qty_2_ratio"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("move_move_type").isNotNull(), 1)).alias("n_move_move_type_is_not_null"),
    )
)

annotated = (
    grouped
    .select(
        "*",
        F.coalesce(F.col("move_operator_2"), F.lit("UNKNOWN")).alias("move_operator_2_or_unknown"),
        F.upper(F.trim(F.col("move_operator_2"))).alias("move_operator_2_clean"),
    )
)

result = (
    annotated
    .select(
        "move_operator_2",
        "opened_on_ym",
        "n_move_type_is_not_null",
        "avg_capacity",
        "move_qty_to_move_qty_2_ratio",
        "row_count",
        "n_move_move_type_is_not_null",
        "move_operator_2_or_unknown",
        "move_operator_2_clean",
    )
    .orderBy(
        F.col("n_move_move_type_is_not_null").asc(),
        "move_operator_2",
        "opened_on_ym",
    )
    .limit(20)
)
