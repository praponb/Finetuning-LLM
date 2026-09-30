from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 8700)
    .select("region", "city")
    .distinct()
)
