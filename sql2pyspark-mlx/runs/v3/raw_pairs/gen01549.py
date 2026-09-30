from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 7]) & ((F.col("last_restock") > "2025-05-08") | F.col("product_id").isin([8, 9, 10])))
    .select(
        "id",
        "quantity",
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
