from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter((F.col("unit_cost") > 158.7) & (F.col("weight_kg") > 12.4))
    .select(
        "category",
        F.col("sku_code").alias("label"),
        F.lit("sku").alias("source"),
    )
)

move_side = (
    df_stock_moves
    .filter(F.col("sku_id") <= 17)
    .select(
        F.col("move_type").alias("category"),
        F.col("operator").alias("label"),
        F.lit("move").alias("source"),
    )
)

result = (
    sku_side
    .unionByName(move_side)
    .orderBy(F.col("source").desc())
)
