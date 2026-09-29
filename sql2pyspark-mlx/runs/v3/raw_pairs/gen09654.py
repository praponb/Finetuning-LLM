from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 2000)
    .select("product_id", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id").isin([3, 4]))
        .select("product_id", "store_id")
    )
)
