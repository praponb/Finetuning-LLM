from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .select("id", "product_id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") >= 104)
        .select("id", "product_id", "quantity")
    )
)
