from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 6, 8]))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

result = (
    filtered
    .select("store_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.month("last_restock").isin([3, 7, 8]))
        .select("store_id", "id")
    )
)
