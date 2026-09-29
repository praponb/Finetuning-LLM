from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("units")
    .orderBy(F.col("units").desc())
)
