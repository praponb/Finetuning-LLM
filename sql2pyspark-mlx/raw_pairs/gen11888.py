from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 7]))
    .select("store_id")
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([1, 2, 4, 6]))
        .select("store_id")
    )
    .distinct()
    .orderBy(F.col("store_id").desc())
)
