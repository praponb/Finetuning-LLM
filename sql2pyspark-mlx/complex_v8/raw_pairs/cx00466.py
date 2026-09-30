from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-03-02").cast("date")) | (F.col("operator").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

with_activity = (
    base_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "INBOUND"), "left_anti")
)

qualified = (
    with_activity.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.move_type").isNotNull()), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "moved_at_date",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.date_format(F.col("moved_at_date"), "MMM yyyy").alias("moved_at_date_label"),
    )
    .orderBy(F.col("move_type").desc())
)
