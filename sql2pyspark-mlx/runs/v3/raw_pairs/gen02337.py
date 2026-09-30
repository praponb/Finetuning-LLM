from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue")
    .unionByName(df_shipments.select(F.col("cost").alias("revenue")))
    .distinct()
    .orderBy("revenue")
)
