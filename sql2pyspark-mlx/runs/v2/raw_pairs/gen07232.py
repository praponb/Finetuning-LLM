from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNull())
    .orderBy("budget", F.col("dept_name").desc())
)
