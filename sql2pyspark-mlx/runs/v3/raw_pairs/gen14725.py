from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 10]) & F.col("store_id").isin([2, 3, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("id").desc())
)
