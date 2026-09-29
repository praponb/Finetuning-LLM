from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") == 32)
    .select("id", "store_id")
    .orderBy(F.col("id").desc())
)
