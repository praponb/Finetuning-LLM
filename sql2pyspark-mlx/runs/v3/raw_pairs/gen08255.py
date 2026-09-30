from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") < "2025-07-01") & (F.col("region") == "North"))
    .select("id", "store_name")
    .orderBy(F.col("id").desc())
)
