from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 1691000)
    .withColumn("location_length", F.length("location"))
)
