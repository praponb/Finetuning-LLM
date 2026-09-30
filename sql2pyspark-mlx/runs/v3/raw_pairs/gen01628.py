from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "budget", "location", "dept_name")
    .orderBy("dept_name", "id")
    .limit(10)
)
