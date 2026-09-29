from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select("id", "city", "sqft", "region")
)
