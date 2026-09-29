from pyspark.sql import functions as F

sku_side = df_skus.select("sku_code", "weight_kg", F.col("category").alias("name"))

move_rows = (
    df_stock_moves
    .filter((F.col("qty") >= 169) & (F.col("warehouse_id") <= 2))
    .select(
        F.col("move_type").alias("sku_code"),
        F.col("unit_price").alias("weight_kg"),
        F.col("operator").alias("name"),
    )
)

move_side = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("sku_id") >= 6))
    .select(
        F.col("move_type").alias("sku_code"),
        F.col("unit_price").alias("weight_kg"),
        F.col("operator").alias("name"),
    )
)

unioned = (
    sku_side
    .unionByName(move_rows)
    .unionByName(move_side)
)

result = (
    unioned
    .groupBy("sku_code")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("weight_kg").alias("total_weight_kg"),
    )
    .orderBy("sku_code")
)
