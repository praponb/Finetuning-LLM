from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("unit_price") > 160.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

move_side = (
    filtered_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("operator").isin("op_dew", "op_bee")))
    .select(
        "move_type",
        F.col("unit_price").alias("amount"),
        F.lit("move").alias("record_type"),
    )
)

sku_side = (
    df_skus
    .filter(F.lower(F.col("sku_code")).like("%l-%"))
    .select("category", "weight_kg", F.lit("sku").alias("record_type"))
)

move_side_2 = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select("move_type", "unit_price", F.lit("move_3").alias("record_type"))
)

result = (
    move_side
    .union(sku_side)
    .union(move_side_2)
    .orderBy("record_type")
)
