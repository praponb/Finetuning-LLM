from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2025-03-15", "2025-10-01"))
    .select("id", "quantity")
    .subtract(
        df_inventory
        .filter(F.year("last_restock") == 2025)
        .select("id", "quantity")
    )
)
