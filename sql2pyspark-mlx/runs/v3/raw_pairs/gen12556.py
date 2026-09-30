from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1209000, 1611000))
    .select("dept_name")
    .distinct()
    .orderBy(F.col("dept_name").desc())
)
