from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 8)
    .select(
        "product_id",
        "store_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("quantity_filled").desc(), F.col("product_id").desc())
)
