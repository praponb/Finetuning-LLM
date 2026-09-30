from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 228)
    .select("quantity")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") >= "2025-04-01")
        .select("quantity")
    )
)
