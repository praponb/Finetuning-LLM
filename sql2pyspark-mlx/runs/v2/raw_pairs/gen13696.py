from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") != 32)
    .select("store_id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("product_id").isin([1, 2]))
        .select("store_id", "product_id")
    )
    .orderBy(F.col("product_id").desc())
)
