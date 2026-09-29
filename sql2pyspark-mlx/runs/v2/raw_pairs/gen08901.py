from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 101)
    .select("id", "store_id", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock").between("2023-08-13", "2024-07-01"))
        .select("id", "store_id", "product_id")
    )
)
