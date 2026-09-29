from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(F.col("budget").between(1507000, 2420000) & (F.col("dept_name") == "HR")))
    .select("location", F.trim("location").alias("clean_location"))
    .orderBy("location")
)
