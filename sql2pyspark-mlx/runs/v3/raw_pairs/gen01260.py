from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2]))
    .select("revenue", F.lit("store_sale").alias("source"))
    .unionByName(
        df_products
        .filter(F.length("product_name") > 6)
        .select(F.col("cost").alias("revenue"), F.lit("product").alias("source"))
    )
    .distinct()
    .orderBy("revenue")
)
