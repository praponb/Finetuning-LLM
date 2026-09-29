from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2023-11-01")
    .select("last_restock", "quantity")
    .subtract(
        df_inventory
        .filter(F.year("last_restock") == 2023)
        .select("last_restock", "quantity")
    )
)
