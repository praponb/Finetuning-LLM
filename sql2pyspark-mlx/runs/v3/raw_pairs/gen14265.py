from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "location", F.lower("location").alias("location_lower"))
    .orderBy(F.col("id").desc())
)
