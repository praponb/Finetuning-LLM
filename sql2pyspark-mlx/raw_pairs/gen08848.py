from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 4017000) & F.col("dept_name").isNull())
    .withColumn("clean_location", F.trim("location"))
    .withColumn("dept_name_lower", F.lower("dept_name"))
    .orderBy(F.col("id").desc())
)
