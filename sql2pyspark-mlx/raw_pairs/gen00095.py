from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 122)
    .select("last_restock", "quantity")
    .unionByName(
        df_inventory
        .filter(F.col("product_id") == 2)
        .select("last_restock", "quantity")
    )
    .distinct()
)
