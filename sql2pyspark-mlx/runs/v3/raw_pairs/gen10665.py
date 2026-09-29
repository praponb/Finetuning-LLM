from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1500, 1700))
    .select("units")
    .unionByName(df_store_sales.filter(F.col("units").between(13, 20)).select("units"))
    .orderBy("units")
)
