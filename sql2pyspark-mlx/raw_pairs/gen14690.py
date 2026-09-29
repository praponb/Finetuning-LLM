from pyspark.sql import functions as F

result = (
    df_inventory
    .select("store_id", "id")
    .intersect(
        df_inventory
        .filter(F.year("last_restock") == 2024)
        .select("store_id", "id")
    )
)
