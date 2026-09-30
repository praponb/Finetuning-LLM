from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") != "Chiang Mai")
    .withColumn("location_title", F.initcap(F.trim("location")))
)
