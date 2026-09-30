from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Bangkok"]) & (F.col("budget") >= 3289000))
    .withColumn("clean_location", F.trim("location"))
    .withColumn("location_dept_name_key", F.concat_ws("-", "location", "dept_name"))
    .orderBy("location_dept_name_key")
)
