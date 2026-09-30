from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-10-01", "2024-12-01"))
    .select(
        "id",
        "last_restock",
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)
