from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 11)
    .select("revenue")
    .subtract(df_shipments.select(F.col("cost").alias("revenue")))
    .orderBy("revenue")
)
