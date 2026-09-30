from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("store_id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("store_id").isin([1, 2, 3, 6]))
        .select("store_id", "product_id")
    )
)
