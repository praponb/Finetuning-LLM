from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "store_id")
    .exceptAll(
        df_inventory
        .filter(F.year("last_restock") >= 2025)
        .select("id", "store_id")
    )
)
