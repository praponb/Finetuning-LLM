from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "dept_id", "hire_date", "manager_id")
    .orderBy("hire_date", "id")
    .limit(20)
)
