from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .select("id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.col("product_id").isin([2, 4, 6, 8]))
        .select("id", "quantity")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)
