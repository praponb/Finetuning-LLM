from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .orderBy(F.col("budget").desc(), "id")
    .limit(5)
)
