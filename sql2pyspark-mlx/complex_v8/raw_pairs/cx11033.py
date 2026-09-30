from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("unit_price") <= 341.5)
    .select("id", F.col("operator").alias("category"))
)

move_rows = (
    df_stock_moves
    .filter((F.col("qty") < 394) & (F.col("sku_id").isNotNull()))
    .select("qty", "operator")
)

result = (
    move_side
    .union(move_rows)
    .distinct()
    .orderBy("id")
)
