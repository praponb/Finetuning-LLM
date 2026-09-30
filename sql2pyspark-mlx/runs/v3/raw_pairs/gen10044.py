from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNotNull())
    .select("quantity", "store_id")
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([2, 4, 5]))
        .select("quantity", "store_id")
    )
    .distinct()
)
