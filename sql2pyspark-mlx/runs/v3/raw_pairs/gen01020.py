from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 124)
    .select("id", "last_restock", "store_id")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") < "2023-10-01")
        .select("id", "last_restock", "store_id")
    )
    .distinct()
    .orderBy(F.col("store_id").desc())
)
