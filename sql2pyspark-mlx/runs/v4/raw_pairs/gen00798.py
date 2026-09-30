from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("budget", "dept_name", F.length("dept_name").alias("dept_name_length"))
    .orderBy("dept_name")
)
