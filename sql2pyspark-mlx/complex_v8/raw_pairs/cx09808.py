from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .select(
        "qty",
        F.col("operator").alias("name"),
        F.col("unit_price").alias("amount"),
        F.lit("move").alias("source"),
    )
)

sku_side = (
    df_skus
    .filter(F.col("category").isin("Apparel", "Beauty", "Electronics"))
    .select(
        "id",
        "sku_code",
        "weight_kg",
        F.lit("sku").alias("source"),
    )
)

sku_rows = (
    df_skus
    .filter((F.length(F.trim(F.col("sku_code"))) >= 4) & (F.col("category").isin("Food", "Apparel", "Toys")))
    .select(
        "id",
        "sku_code",
        "weight_kg",
        F.lit("sku").alias("source"),
    )
)

combined = (
    move_rows
    .union(sku_side)
    .union(sku_rows)
)

result = (
    combined
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("amount").alias("total_amount"))
    .orderBy("source")
)
