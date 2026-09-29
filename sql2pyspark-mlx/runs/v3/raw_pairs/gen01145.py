from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 3600)
    .select("product_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([4, 7, 8, 10]))
        .select("product_id")
    )
    .orderBy(F.col("product_id").desc())
)
