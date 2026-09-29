from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 4, 11]))
    .select("store_id", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") < 237)
        .select("store_id", "product_id")
    )
    .distinct()
)
