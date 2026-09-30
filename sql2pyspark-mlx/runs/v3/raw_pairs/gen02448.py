from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .select("quantity")
    .unionByName(
        df_inventory
        .filter(F.col("product_id").isin([5, 6, 7]))
        .select("quantity")
    )
    .distinct()
)
