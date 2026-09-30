from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2023-06-01")
    .select("id", "store_id")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") <= "2023-07-01")
        .select("id", "store_id")
    )
    .orderBy("id")
)
