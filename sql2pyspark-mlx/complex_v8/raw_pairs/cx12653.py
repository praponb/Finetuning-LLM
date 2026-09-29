from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.col("unit_price") <= 330.9)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label_2"),
    )
)

annotated = (
    enriched
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator",
        "move_type_clean",
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
    )
)

result = (
    annotated
    .groupBy("move_type")
    .pivot("operator", ["op_chan", "op_bee"])
    .agg(F.sum("moved_at_quarter"))
)
