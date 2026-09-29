from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .unionByName(df_shipments.select(F.col("cost").alias("price")))
    .distinct()
    .orderBy("price")
)
