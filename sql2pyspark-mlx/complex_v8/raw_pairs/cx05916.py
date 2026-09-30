from pyspark.sql import functions as F

base_skus = (
    df_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        F.when(F.col("unit_cost") >= 251.2, "HIGH").when(F.col("unit_cost") >= 134.3, "MEDIUM").otherwise("LOW").alias("unit_cost_tier"),
    )
)

sku_side = (
    base_skus
    .select(
        F.col("unit_cost_tier").alias("label"),
        "id",
        F.lit("sku").alias("record_type"),
    )
)

sku_rows = df_skus.select("sku_code", "id", F.lit("sku_2").alias("record_type"))

move_rows = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select("operator", "sku_id", F.lit("move").alias("record_type"))
)

result = (
    sku_side
    .union(sku_rows)
    .union(move_rows)
    .orderBy(F.col("label").desc())
)
