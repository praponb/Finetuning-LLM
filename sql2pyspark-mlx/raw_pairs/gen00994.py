from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Tokyo")
    .select("dept_name")
    .distinct()
    .orderBy(F.col("dept_name").desc())
)
