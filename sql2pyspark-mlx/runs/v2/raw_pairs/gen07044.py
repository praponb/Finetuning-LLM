from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "Legal"]) & (F.col("location") == "Chiang Mai"))
    .withColumn("location_lower", F.lower("location"))
    .orderBy("location")
)
