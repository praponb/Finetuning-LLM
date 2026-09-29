from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2003000)
    .select("dept_name")
    .intersect(df_departments.filter(F.col("budget") < 3418000).select("dept_name"))
    .orderBy("dept_name")
)
