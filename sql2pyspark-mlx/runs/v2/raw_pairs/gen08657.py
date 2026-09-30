from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select("id", "hire_date", "salary", "manager_id")
    .orderBy("manager_id")
)
