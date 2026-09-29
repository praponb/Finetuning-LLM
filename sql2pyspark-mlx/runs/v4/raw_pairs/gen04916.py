from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 5, 6]))
    .select("id", "units")
    .orderBy(F.col("units").desc(), F.col("id").desc())
)
