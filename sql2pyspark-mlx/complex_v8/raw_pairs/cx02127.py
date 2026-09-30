from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

result = (
    scoped_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 329.7, "A").when(F.col("unit_price") >= 150.7, "B").otherwise("C").alias("unit_price_tier"),
        F.when(F.col("qty") >= 348, "HIGH").when(F.col("qty") >= 152, "MEDIUM").otherwise("LOW").alias("qty_tier"),
    )
    .orderBy(F.col("unit_price_tier").desc_nulls_first(), "id")
    .limit(10)
)
