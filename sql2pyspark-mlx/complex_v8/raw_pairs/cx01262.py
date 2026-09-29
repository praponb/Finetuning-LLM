from pyspark.sql import functions as F

filtered_skus = (
    df_skus
    .filter((F.array_contains(F.col("labels"), "hazmat")) & (F.col("category") != "Beauty"))
)

sku_side = (
    filtered_skus
    .filter((F.upper(F.trim(F.col("sku_code"))).like("E%")) & (F.col("unit_cost") < 302.5))
    .select("id", F.col("weight_kg").alias("metric"), F.col("category").alias("name"))
)

move_rows = (
    df_stock_moves
    .filter((F.col("move_type") == "OUTBOUND") & (F.col("warehouse_id") >= 2))
    .select(
        F.col("qty").alias("id"),
        F.col("unit_price").alias("metric"),
        F.col("operator").alias("name"),
    )
)

sku_side_2 = df_skus.select("id", F.col("weight_kg").alias("metric"), F.col("category").alias("name"))

result = (
    sku_side
    .unionByName(move_rows)
    .unionByName(sku_side_2)
    .orderBy("id", "metric")
)
