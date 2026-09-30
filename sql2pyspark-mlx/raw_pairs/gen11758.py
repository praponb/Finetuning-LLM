from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("price").alias("value"))
    .unionByName(df_shipments.select(F.col("cost").alias("value")))
    .distinct()
    .orderBy("value")
)
