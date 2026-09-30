from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 3)
    .select("id", "product_id", "store_id")
    .intersect(
        df_inventory
        .filter(F.col("store_id").isin([3, 6]))
        .select("id", "product_id", "store_id")
    )
    .orderBy("product_id")
)
