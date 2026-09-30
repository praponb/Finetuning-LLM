from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([5, 6]) | F.col("last_restock").between("2023-08-01", "2024-11-17"))
    .select(
        "id",
        "store_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.round("quantity", 2).alias("quantity_rounded"),
    )
    .orderBy(F.col("quantity_rounded").desc(), "id")
    .limit(5)
)
