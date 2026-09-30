from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "store_name")
    .orderBy(F.col("id").desc(), "store_name")
)
