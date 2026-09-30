from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "budget", "location", F.length("location").alias("location_length"))
    .orderBy(F.col("budget").desc(), F.col("location_length").desc())
)
