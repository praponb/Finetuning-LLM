from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .select("units", "product_id")
    .unionByName(
        df_store_sales
        .filter(F.col("revenue") <= 2800)
        .select("units", "product_id")
    )
    .distinct()
)
