from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.col("move_type").isin("ADJUST", "TRANSFER", "OUTBOUND"))
    .select(F.col("sku_id").alias("entity_id"), F.col("operator").alias("label"))
)

sku_rows = (
    df_skus
    .filter(F.col("weight_kg") < 21.0)
    .select("id", "sku_code")
)

result = (
    move_rows
    .subtract(sku_rows)
    .orderBy("entity_id", "label")
)
