from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-02-01", "2025-03-29"))
    .select("id", "product_id")
    .intersect(
        df_inventory
        .filter(F.year("last_restock") == 2024)
        .select("id", "product_id")
    )
    .orderBy("id")
)
