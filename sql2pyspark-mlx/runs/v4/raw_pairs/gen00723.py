from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select("is_remote", "dept_id", "full_name")
)
