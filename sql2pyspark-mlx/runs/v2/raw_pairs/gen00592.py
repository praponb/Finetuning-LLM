from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 155)
    .select("id", "store_id")
    .intersect(
        df_inventory
        .filter(F.col("last_restock") > "2024-04-04")
        .select("id", "store_id")
    )
)
