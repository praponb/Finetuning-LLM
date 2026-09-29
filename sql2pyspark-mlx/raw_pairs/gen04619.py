from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNotNull())
    .select(F.lit("product").alias("origin"), F.col("cost").alias("value"))
    .unionByName(
        df_store_sales
        .filter(F.col("revenue").isNotNull())
        .select(F.lit("store_sale").alias("origin"), F.col("revenue").alias("value"))
    )
    .orderBy("origin")
)
