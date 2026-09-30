from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue", F.lit("store_sale").alias("source"))
    .unionByName(
        df_products
        .filter(F.col("price") > 970)
        .select(F.col("price").alias("revenue"), F.lit("product").alias("source"))
    )
    .orderBy("revenue")
)
