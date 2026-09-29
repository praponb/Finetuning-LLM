from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .select(
        F.col("id").alias("entity_id"),
        "unit_price",
        F.lit("move").alias("origin"),
    )
)

move_side = (
    df_stock_moves
    .filter(F.col("move_type").isin("ADJUST", "OUTBOUND"))
    .select("id", "unit_price", F.lit("move_2").alias("origin"))
)

sku_side = df_skus.select("id", "weight_kg", F.lit("sku").alias("origin"))

stacked = (
    move_rows
    .union(move_side)
    .union(sku_side)
)

result = (
    stacked
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy("origin")
)
