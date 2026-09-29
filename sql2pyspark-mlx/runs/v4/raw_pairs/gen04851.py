from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower(F.trim("store_name")) == "old town")
    .select("region")
    .distinct()
)
