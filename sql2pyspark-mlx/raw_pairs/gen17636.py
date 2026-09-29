from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2023-09-23")
    .select("id", "last_restock", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") < 181)
        .select("id", "last_restock", "product_id")
    )
)
