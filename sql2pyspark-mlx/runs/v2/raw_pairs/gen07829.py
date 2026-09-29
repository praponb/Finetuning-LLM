from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 2)
    .select("product_id", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id") == 7)
        .select("product_id", "store_id")
    )
    .orderBy(F.col("product_id").desc())
)
