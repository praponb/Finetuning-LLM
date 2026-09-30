from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 6]))
    .select("product_id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") <= 77)
        .select("product_id", "quantity")
    )
    .distinct()
)
