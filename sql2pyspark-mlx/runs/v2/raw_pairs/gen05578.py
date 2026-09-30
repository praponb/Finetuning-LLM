from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.year("hire_date") >= 2023)
    .select("dept_id")
    .subtract(df_employees.filter(F.col("email").isNotNull()).select("dept_id"))
)
