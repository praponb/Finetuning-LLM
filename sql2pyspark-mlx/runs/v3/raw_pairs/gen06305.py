from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 4016000)
    .select("id", "dept_name", "budget")
    .orderBy("budget")
)
