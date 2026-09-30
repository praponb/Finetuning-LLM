from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1917000)
    .withColumn("location_lower", F.lower("location"))
    .withColumn("location_lower_2", F.lower("location"))
)
