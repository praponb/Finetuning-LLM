from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 146)
    .select("last_restock", "product_id")
    .intersect(
        df_inventory
        .filter(F.col("store_id").isin([1, 4, 5, 6]))
        .select("last_restock", "product_id")
    )
    .orderBy(F.col("last_restock").desc())
)
