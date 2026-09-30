from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3719000)
    .select("dept_name", "budget")
    .orderBy(F.col("dept_name").desc())
)
