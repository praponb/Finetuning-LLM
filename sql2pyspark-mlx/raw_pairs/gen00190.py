from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2765000)
    .orderBy("dept_name")
)
