from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 128000)
    .select("id", "is_remote", "dept_id", "manager_id")
)
