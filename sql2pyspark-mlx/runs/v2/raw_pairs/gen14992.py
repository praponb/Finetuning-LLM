from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 6)
    .select("id", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("product_id") == 5)
        .select("id", "product_id")
    )
)
