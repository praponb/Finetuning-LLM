from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2]))
    .select("quantity")
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([1, 3, 6]))
        .select("quantity")
    )
    .distinct()
    .orderBy("quantity")
)
