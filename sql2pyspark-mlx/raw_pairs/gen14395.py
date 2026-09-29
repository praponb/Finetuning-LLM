from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.lit("store_sale").alias("origin"), F.col("revenue").alias("value"))
    .unionByName(
        df_products
        .select(F.lit("product").alias("origin"), F.col("price").alias("value"))
    )
    .orderBy("value")
)
