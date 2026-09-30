from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 216)
    .select("product_id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") > "2024-02-24")
        .select("product_id", "quantity")
    )
    .distinct()
)
