from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3]))
    .select("product_id", "store_id")
    .intersect(
        df_inventory
        .filter(F.col("product_id").isin([1, 3, 5, 7]))
        .select("product_id", "store_id")
    )
)
