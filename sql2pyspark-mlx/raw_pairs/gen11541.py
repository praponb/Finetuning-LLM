from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 6]))
    .select("store_id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.year("last_restock") >= 2025)
        .select("store_id", "quantity")
    )
    .orderBy("quantity")
)
