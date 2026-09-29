from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-02-18 15:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

annotated = (
    base
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.when(F.col("qty") >= 245, "large").when(F.col("qty") >= 208, "medium").otherwise("small").alias("qty_tier"),
    )
)

annotated_2 = (
    annotated
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "qty_tier",
        F.date_trunc("quarter", F.col("moved_at")).alias("moved_at_quarter"),
        F.concat_ws(" / ", F.col("move_type"), F.col("qty_tier")).alias("move_type_qty_tier_label"),
    )
)

result = (
    annotated_2
    .groupBy("sku_id", "moved_at_quarter")
    .pivot("move_type", ["OUTBOUND", "ADJUST", "INBOUND", "TRANSFER"])
    .agg(F.sum("qty"))
)
