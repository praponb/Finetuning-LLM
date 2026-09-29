from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 135000) & F.col("manager_id").isNotNull())
    .select("email", "dept_id", "hire_date")
)
