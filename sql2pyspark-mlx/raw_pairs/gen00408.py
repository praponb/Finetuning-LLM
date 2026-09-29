from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("store_id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("product_id").isin([8, 10]))
        .select("store_id", "product_id")
    )
)
