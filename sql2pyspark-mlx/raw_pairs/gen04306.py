from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("quantity") > 173)
    .select(
        "id",
        "product_id",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .cube(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.quantity_filled")).alias("min_quantity_filled"),
    )
)
