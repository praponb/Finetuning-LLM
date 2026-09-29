from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .select("revenue")
    .unionByName(
        df_products
        .filter(F.col("cost").isNotNull())
        .select(F.col("cost").alias("revenue"))
    )
    .distinct()
)
