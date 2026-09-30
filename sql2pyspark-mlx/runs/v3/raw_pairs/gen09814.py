from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "dept_name", "budget", "location")
    .orderBy("budget", "id")
    .limit(50)
)
